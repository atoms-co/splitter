package model

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	"atoms.co/lib-go/pkg/clock"
	"go.atoms.co/splitter/lib/service/location"
	"go.atoms.co/lib/log"
	"go.atoms.co/lib/metrics"
	"go.atoms.co/lib/timex"
	"go.atoms.co/lib/chanx"
	"go.atoms.co/lib/contextx"
	"go.atoms.co/lib/net/grpcx"
	"go.atoms.co/lib/iox"
	"go.atoms.co/lib/mapx"
	"go.atoms.co/lib/randx"
	"go.atoms.co/slicex"
	"go.atoms.co/lib/syncx"
)

const (
	statsDuration = 15 * time.Second
)

var (
	numGrants = metrics.NewTrackedGauge(metrics.NewGauge("go.atoms.co/splitter/client/workpool_grants", "Workpool grants", slicex.CopyAppend(qualifiedDomainKeys, sourceKey, sourceVersionKey, leaseStateKey)...))
)

type JoinFn func(ctx context.Context, self location.Instance, handler grpcx.Handler[ConsumerMessage, ConsumerMessage]) error

type joinStatus struct {
	connected time.Time
}

// WorkPool manages a pool of grants assigned by Splitter to the consumer and provides updated clusters. It maintains
// WorkPool with the service, re-connecting on failures, and continues to manage assigned grants when disconnected.
//
// Shutdown is initiated by calling Drain(). Closed() can be used to detect when draining has stopped and
// the pool is closed.
type WorkPool struct {
	iox.RAsyncCloser

	self    Consumer
	service QualifiedServiceName
	domains []QualifiedDomainName
	joinFn  JoinFn
	handler Handler
	opts    []ConsumerOption

	status *joinStatus            // coordinator connectivity status
	in     <-chan ConsumerMessage // coordinator incoming messages (empty and not closed, if disconnected)
	out    chan<- ConsumerMessage // coordinator outgoing messages (empty and not closed, if disconnected)

	cluster  *ClusterMap
	clusters chan Cluster

	grants map[GrantID]*grant
	shards map[Shard]GrantID
	lease  *timex.Timer
	expire chan bool

	inject chan func()
	quit   iox.AsyncCloser
	drain  iox.AsyncCloser
}

func NewWorkPool(consumer Consumer, service QualifiedServiceName, domains []QualifiedDomainName, joinFn JoinFn, handlerFn Handler, opts ...ConsumerOption) (*WorkPool, <-chan Cluster) {
	quit := iox.NewAsyncCloser()
	p := &WorkPool{
		RAsyncCloser: quit,
		self:         consumer,
		service:      service,
		domains:      domains,
		joinFn:       joinFn,
		handler:      handlerFn,
		opts:         opts,
		cluster:      NewClusterMap(NewClusterID(consumer.Instance(), time.Now()), nil), // empty self-origin map
		clusters:     make(chan Cluster, 1),
		grants:       map[GrantID]*grant{},
		shards:       map[Shard]GrantID{},
		expire:       make(chan bool, 1),
		inject:       make(chan func()),
		quit:         quit,
		drain:        iox.WithQuit(quit.Closed(), iox.NewAsyncCloser()),
	}

	ctx := NewConsumerContext(context.Background(), consumer)
	go p.join(ctx)
	go p.process(ctx)
	return p, p.clusters
}

func (p *WorkPool) Drain(timeout time.Duration) {
	ctx := NewConsumerContext(context.Background(), p.self)
	log.Infof(ctx, "Draining WorkPool with timeout %v", timeout)
	now := time.Now()
	p.drain.Close()
	go func() {
		timeoutTimer := time.NewTimer(timeout)
		defer timeoutTimer.Stop()
		select {
		case <-timeoutTimer.C:
			log.Infof(ctx, "WorkPool closed forcefully after draining for %v", time.Since(now))
			p.quit.Close()
		case <-p.quit.Closed():
			log.Infof(ctx, "WorkPool closed after draining for %v", time.Since(now))
		}
	}()
}

func (p *WorkPool) join(ctx context.Context) {
	defer func() {
		log.Infof(ctx, "Finishing WorkPool reconnection to the coordinator")
	}()

	wctx, _ := contextx.WithQuitCancel(ctx, p.Closed())

	for !p.drain.IsClosed() {
		// Not connected. Establish WorkPool with coordinator with blocking join call. The workpool is either
		// connecting or connected while in the join call.

		log.Infof(ctx, "WorkPool attempting to join coordinator")

		err := p.joinFn(wctx, p.self.Instance(), func(ctx context.Context, in <-chan ConsumerMessage) (<-chan ConsumerMessage, error) {
			return syncx.Txn1(ctx, p.txn, func() (<-chan ConsumerMessage, error) {
				return p.joinCoordinator(ctx, in)
			})
		})

		if err != nil && !errors.Is(err, io.EOF) {
			log.Infof(ctx, "WorkPool disconnected from coordinator: %v", err)
		} else {
			log.Infof(ctx, "WorkPool disconnected from coordinator")
		}

		syncx.Txn0(wctx, p.txn, func() {
			p.lostCoordinator(ctx)
		})

		time.Sleep(time.Second + randx.Duration(time.Second)) // short random backoff on disconnect
	}
}

func (p *WorkPool) joinCoordinator(ctx context.Context, in <-chan ConsumerMessage) (<-chan ConsumerMessage, error) {
	p.lostCoordinator(ctx) // sanity check

	active := mapx.MapValuesIf(p.grants, func(g *grant) (Grant, bool) {
		return g.ToUpdated(), g.LeaseState == LeaseStale
	})

	log.Infof(ctx, "Connected to coordinator, #grants=%v, #active=%v", len(p.grants), len(active))

	out := make(chan ConsumerMessage, 2_000)
	out <- NewRegister(p.self, p.service, p.domains, active, p.opts...)

	p.in = in
	p.out = out
	p.status = &joinStatus{
		connected: time.Now(),
	}
	p.lease = nil

	return out, nil
}

func (p *WorkPool) lostCoordinator(ctx context.Context) {
	if p.status == nil {
		return // ok: already disconnected
	}

	log.Infof(ctx, "Lost connection to coordinator, connected=%v, #grants=%v", time.Since(p.status.connected), len(p.grants))

	close(p.out)

	p.in = make(chan ConsumerMessage)
	p.out = make(chan ConsumerMessage)
	p.status = nil
	p.lease = nil
	leases := map[time.Time]*timex.Timer{}

	for _, g := range p.grants {
		if g.LeaseState != LeaseRevoked {
			g.LeaseState = LeaseStale
		}
		// Revoke all non-revoked grants if the work pool is draining. It will not reconnect to the server
		// after disconnecting in this case, and it's safe to revoke locally.
		if p.drain.IsClosed() && !IsRevokedOrUnloaded(g.Grant.State()) {
			p.revokeGrant(ctx, leases, g, g.Grant.WithState(RevokedGrantState))
		}
	}
}

func (p *WorkPool) process(ctx context.Context) {
	defer p.quit.Close()
	defer func() {
		log.Infof(ctx, "Finishing WorkPool processing")
	}()

	statsTimer := time.NewTicker(statsDuration)
	defer statsTimer.Stop()

steady:
	for {
		select {
		case msg, ok := <-p.in:
			if !ok {
				log.Warnf(ctx, "Coordinator connection unexpectedly closed")
				p.lostCoordinator(ctx)
				break
			}
			p.handleMessage(ctx, msg)

		case <-p.expire:
			p.checkExpiration(ctx)

		case fn := <-p.inject:
			fn()

		case <-statsTimer.C:
			// record metrics
			log.Infof(ctx, "WorkPool %v, joined=%v, #shards=%v, #grants=%v", p.self, p.status != nil, len(p.shards), len(p.grants))
			p.emitMetrics(ctx)

		case <-p.drain.Closed():
			break steady

		case <-p.Closed():
			log.Infof(ctx, "Aborting WorkPool processing due to closure")
			return
		}
	}

	log.Infof(ctx, "WorkPool %v draining, joined=%v, #grants=%v", p.self, p.status != nil, len(p.grants))
	now := time.Now()

	if p.status == nil || !p.trySend(ctx, NewDeregister()) {
		log.Infof(ctx, "WorkPool is draining while disconnected/stuck")
	}

drain:
	for {
		select {
		case msg, ok := <-p.in:
			if !ok {
				if len(p.grants) == 0 {
					log.Infof(ctx, "Workpool drained after %v", time.Since(now))
					p.lostCoordinator(ctx)
					return
				}
				log.Warnf(ctx, "Coordinator connection unexpectedly closed while draining")
				p.lostCoordinator(ctx)
				break drain
			}
			p.handleMessage(ctx, msg)

		case <-p.expire:
			p.checkExpiration(ctx)

		case fn := <-p.inject:
			fn()

		case <-p.Closed():
			log.Infof(ctx, "Aborting WorkPool processing due to closure")
			return
		}
	}

	log.Infof(ctx, "Waiting for remainig #%v grants to expire", len(p.grants))

	for {
		if len(p.grants) == 0 {
			break
		}

		select {
		case <-p.expire:
			p.checkExpiration(ctx)

		case fn := <-p.inject:
			fn()

		case <-p.Closed():
			return
		}
	}
}

func (p *WorkPool) handleMessage(ctx context.Context, msg ConsumerMessage) {
	switch {
	case msg.IsClientMessage():
		client, _ := msg.ClientMessage()
		p.handleClientMessage(ctx, client)

	case msg.IsClusterMessage():
		cluster, _ := msg.ClusterMessage()
		p.handleClusterMessage(ctx, cluster)

	default:
		log.Errorf(ctx, "Internal: unexpected coordinator message: %v", msg)
	}
}

func (p *WorkPool) handleClientMessage(ctx context.Context, msg ClientMessage) {
	switch {
	case msg.IsAssign():
		assign, _ := msg.Assign()
		grants := assign.Grants()

		for _, g := range grants {
			p.shards[g.Shard()] = g.ID()
			if old, ok := p.grants[g.ID()]; ok {
				if old.LeaseState == LeaseStale {
					if p.updateStaleGrant(ctx, old, g) {
						log.Infof(ctx, "Re-activating stale grant %v", old)
						old.LeaseState = LeaseActive
						old.Lease = p.lease
						log.Infof(ctx, "Re-activated stale grant: %v", old)
						continue
					}
					log.Errorf(ctx, "Internal: unexpected assignment of grant in invalid state. Re-creating. Old grant: %v. New grant: %v", old, g)
				} else {
					log.Errorf(ctx, "Internal: unexpected re-grant of non-stale grant %v. Re-creating", old)
				}
				old.Handler.Close()
				delete(p.grants, g.ID())
			}

			gr := &grant{
				Grant:      g,
				LeaseState: LeaseActive,
				Lease:      p.lease,
			}
			h := newHandler(ctx, clock.New(), g, gr.Expiration, newLoader(), newUnloader(), p.handler)
			gr.Handler = h
			p.grants[g.ID()] = gr

			// optional: wait for client to ask to revoke the grant
			go func() {
				select {
				case <-h.ownership.revokeRequested.Closed(): // client requested to revoke the grant:
					syncx.Txn0(ctx, p.txn, func() {
						p.releaseGrant(ctx, g.ID())
					})
				case <-h.Closed():
					return
				}
			}()

			// Wait on client Load
			go func() {
				select {
				case <-h.ownership.loader.loaded().Closed():
					syncx.Txn0(ctx, p.txn, func() {
						if grant, ok := p.grants[g.ID()]; ok && grant.Grant.State() == AllocatedGrantState {
							grant.Grant = grant.ToState(LoadedGrantState)
							log.Infof(ctx, "Informing the coordinator of grant load %v", grant.Grant)
							p.mustSend(ctx, NewUpdate(grant.Grant))
						}
					})
					return
				case <-h.Closed():
					return
				}
			}()

			// Wait on client Unload
			go func() {
				select {
				case <-h.ownership.unloader.unloaded().Closed(): // client closed ownership.Unloader.Unload()
					syncx.Txn0(ctx, p.txn, func() {
						if grant, ok := p.grants[g.ID()]; ok && grant.Grant.State() == RevokedGrantState {
							grant.Grant = grant.ToState(UnloadedGrantState)
							log.Infof(ctx, "Informing the coordinator of grant unload %v", grant.Grant)
							p.mustSend(ctx, NewUpdate(grant.Grant))
						}
					})
					return
				case <-h.Closed():
					return
				}
			}()

			go func() {
				select {
				case <-h.Closed():
					syncx.Txn0(ctx, p.txn, func() {
						p.removeGrant(ctx, g.ID())
					})
				case <-p.Closed():
					h.Close()
				}
			}()

			log.Infof(ctx, "Created handler for grant %v", g)
		}

	case msg.IsPromote():
		promote, _ := msg.Promote()
		grants := promote.Grants()

		for _, g := range grants {
			p.shards[g.Shard()] = g.ID()
			candidate, ok := p.grants[g.ID()]
			if !ok {
				log.Errorf(ctx, "Internal: unexpected promotion of unowned grant %v. Releasing promotion", g)
				p.mustSend(ctx, NewReleased(g))
				continue
			}
			if candidate.LeaseState != LeaseActive {
				log.Errorf(ctx, "Internal: unexpected promotion of inactive grant %v. Releasing promotion and closing grant", g)
				p.mustSend(ctx, NewReleased(g))
				candidate.Handler.Close()
				continue
			}
			if candidate.Grant.State() != AllocatedGrantState && candidate.Grant.State() != LoadedGrantState {
				log.Errorf(ctx, "Internal: unexpected promotion of non-allocated grant %v. Releasing promotion and closing grant", g)
				p.mustSend(ctx, NewReleased(g))
				candidate.Handler.Close()
				continue
			}
			p.activateGrant(ctx, candidate, g)
		}

	case msg.IsRevoke():
		revoke, _ := msg.Revoke()
		grants := revoke.Grants()
		leases := map[time.Time]*timex.Timer{}

		for _, g := range grants {
			grant, ok := p.grants[g.ID()]
			if !ok {
				log.Warnf(ctx, "Revoking stale grant: %v. Ignoring", g)
				continue
			}
			p.revokeGrant(ctx, leases, grant, g)
		}

	case msg.IsNotify():
		notify, _ := msg.Notify()
		update, target := notify.Update(), notify.Target()

		log.Infof(ctx, "Received grant update %v, for target %v", update, target)

		g, ok := p.grants[target.ID()]
		if !ok {
			log.Warnf(ctx, "Updating stale grant: %v. Ignoring", target)
			break
		}

		switch update.State() {
		case LoadedGrantState:
			g.Handler.ownership.unloader.load() // Notify client that grant was loaded
		case UnloadedGrantState:
			g.Handler.ownership.loader.unload() // Notify client that grant was unloaded
		default:
			log.Warnf(ctx, "Received grant update with unexpected state: %v", update)
		}

	case msg.IsExtend():
		extend, _ := msg.Extend()

		if p.lease == nil {
			p.lease = timex.AfterFunc(time.Until(extend.Lease()), p.emitExpirationCheck)
		}
		p.lease.Reset(time.Until(extend.Lease()))
	default:
		log.Errorf(ctx, "Unexpected coordinator message: %v", msg)
	}
}

func (p *WorkPool) activateGrant(ctx context.Context, grant *grant, active Grant) {
	grant.Grant = active               // Overwrite allocated grant with active grant
	grant.Handler.ownership.activate() // Signal consumer that grant is activated

	log.Infof(ctx, "Promoted grant to active: %v", active)
}

func (p *WorkPool) revokeGrant(ctx context.Context, leases map[time.Time]*timex.Timer, grant *grant, revoked Grant) {
	// (1) Create lease that expires at the passed grant expiration. The workpool lease no longer includes this grant.
	// We share the leases to not get a flood of identical expiration checks.

	ttl := revoked.Lease()
	if _, ok := leases[ttl]; !ok {
		leases[ttl] = timex.AfterFunc(time.Until(ttl), p.emitExpirationCheck)
	}
	grant.Lease = leases[ttl]
	grant.LeaseState = LeaseRevoked
	grant.Grant = revoked            // Overwrite activated grant with revoked grant
	grant.Handler.ownership.revoke() // Signal consumer that grant is revoked

	// (2) Revoke async. If the Revoke unexpectedly arrives before Assign, the lease still expires.

	log.Infof(ctx, "Revoking grant %v, ttl=%v", revoked, ttl)

	grant.Handler.Drain(time.Until(ttl))
}

func (p *WorkPool) updateStaleGrant(ctx context.Context, old *grant, newGrant Grant) bool {
	if old.Grant.State() == newGrant.State() {
		return true
	}

	switch newGrant.State() {
	case AllocatedGrantState:
		// Grant advanced locally, but server grant is still allocated. The update will eventually
		// reach the server.
		if old.Grant.State() == LoadedGrantState {
			return true
		}
		return false
	case LoadedGrantState:
		return false // new grant can't be loaded without old being loaded
	case ActiveGrantState:
		// only allocated grant can be activated
		if !IsAllocatedOrLoaded(old.Grant.State()) {
			return false
		}
		p.activateGrant(ctx, old, newGrant)
		return true
	case RevokedGrantState, UnloadedGrantState:
		// Grants are revoked explicitly using revoke message, not assign.
		return false
	default:
		return false
	}
}

func (p *WorkPool) handleClusterMessage(ctx context.Context, msg ClusterMessage) {
	// Cluster update. Merge with current cluster and forward to listeners.

	upd, err := UpdateClusterMap(ctx, p.cluster, msg)
	if err != nil {
		log.Errorf(ctx, "Internal: unexpected cluster message %v: %v. Disconnecting", msg, err)
		p.lostCoordinator(ctx)
		return
	}

	p.cluster = upd

	chanx.Clear(p.clusters)
	p.clusters <- upd

}

func (p *WorkPool) emitExpirationCheck() {
	select {
	case <-p.expire:
	default:
	}
	p.expire <- true
}

func (p *WorkPool) checkExpiration(ctx context.Context) {
	// Possible grant expiration

	now := time.Now()
	for gid, g := range p.grants {
		if g.Expiration().Before(now) {
			p.removeGrant(ctx, gid)
		}
	}
}

func (p *WorkPool) removeGrant(ctx context.Context, gid GrantID) {
	grant, ok := p.grants[gid]
	if !ok {
		return // ok: no longer present
	}
	grant.Handler.Close()

	if grant.LeaseState != LeaseStale && time.Now().Before(grant.Expiration()) {
		p.mustSend(ctx, NewReleased(grant.Grant))
	}
	delete(p.grants, gid)
	if p.shards[grant.Grant.Shard()] == gid {
		delete(p.shards, grant.Grant.Shard()) // delete service tracking if not replaced by new grant
	}

	log.Infof(ctx, "Removed grant %v", grant)
}

func (p *WorkPool) releaseGrant(ctx context.Context, gid GrantID) {
	g, ok := p.grants[gid]
	if !ok {
		return // ok: no longer present
	}

	if g.LeaseState == LeaseActive {
		// Consumer tries to notify the coordinator to revoke the grant, the message is not guaranteed to be delivered.
		// Range must also release a grant after some period of time so that it's eventually revoked.
		if p.trySend(ctx, NewRevoke(g.Grant)) {
			log.Infof(ctx, "Requested to release grant %v", g)
		} else {
			log.Warnf(ctx, "Unabled to request to release grant %v", g)
		}
	}
}

func (p *WorkPool) emitMetrics(ctx context.Context) {
	p.resetMetrics(ctx)

	grants := map[QualifiedDomainName]map[LeaseState]int{}
	for _, grant := range p.grants {
		domain := grant.Grant.Shard().Domain
		if grants[domain] == nil {
			grants[domain] = map[LeaseState]int{}
		}
		grants[domain][grant.LeaseState] += 1
	}

	for domain, counts := range grants {
		for state, count := range counts {
			numGrants.Set(ctx, float64(count), slicex.CopyAppend(qualifiedDomainTags(domain), sourceTag(), sourceVersionTag(ClientVersion), leaseStateTag(state))...)
		}
	}
}

func (p *WorkPool) resetMetrics(ctx context.Context) {
	numGrants.Reset(ctx)
}

func (p *WorkPool) trySend(ctx context.Context, msg ConsumerMessage) bool {
	select {
	case p.out <- msg:
		return true
	default:
		return false
	}
}

func (p *WorkPool) mustSend(ctx context.Context, msg ConsumerMessage) {
	if p.status == nil {
		return // ok: disconnected
	}

	timeout := 100 * time.Millisecond
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case p.out <- msg:
		return
	case <-timer.C:
		log.Warnf(ctx, "Unabled to send message to the coordinator in %v. Disconnecting", timeout)
		p.lostCoordinator(ctx)
	}
}

func (p *WorkPool) txn(ctx context.Context, fn func() error) error {
	var wg sync.WaitGroup
	var err error

	wg.Add(1)
	select {
	case p.inject <- func() {
		defer wg.Done()
		err = fn()
	}:
		wg.Wait()
		return err
	case <-ctx.Done():
		return ErrOverloaded
	case <-p.Closed():
		return ErrDraining
	}
}
