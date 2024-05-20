package model

import (
	"context"
	"atoms.co/lib-go/pkg/clock"
	"go.atoms.co/splitter/lib/service/location"
	"go.atoms.co/lib/log"
	"go.atoms.co/lib/metrics"
	"go.atoms.co/lib/chanx"
	"go.atoms.co/lib/clockx"
	"go.atoms.co/lib/contextx"
	"go.atoms.co/lib/net/grpcx"
	"go.atoms.co/lib/iox"
	"go.atoms.co/lib/mapx"
	"go.atoms.co/lib/randx"
	"go.atoms.co/slicex"
	"go.atoms.co/lib/syncx"
	"sync"
	"time"
)

const (
	statsDuration = 15 * time.Second
)

var (
	numGrants = metrics.NewTrackedGauge(metrics.NewGauge("go.atoms.co/splitter/client/workpool_grants", "Workpool grants", slicex.CopyAppend(qualifiedDomainKeys, leaseStateKey)...))
)

type JoinFn func(ctx context.Context, self location.Instance, handler grpcx.Handler[ConsumerMessage, ConsumerMessage]) error

type joinStatus struct {
	connected time.Time
}

// WorkPool manages a pool of grants assigned by Splitter to the consumer and provides updated clusters. It maintains
// WorkPool with the service, re-connecting on failures, and continues to manage assigned grants when disconnected.
//
// Life cycle is controlled by the context passed on creation; context cancellation triggers draining procedure.
// `Drained()` can be used to detect when draining has stopped and the pool is being closed.
// `Closed()` can be used to detect when draining has stopped and the pool is closed. The pool can close itself when
// the lease is expired, and it wasn't renewed by the service for some reason.
type WorkPool struct {
	iox.AsyncCloser

	cl      clock.Clock
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
	lease  *clockx.Timer
	expire chan bool

	inject chan func()
	quit   iox.AsyncCloser
	drain  iox.AsyncCloser
}

func NewWorkPool(cl clock.Clock, consumer Consumer, service QualifiedServiceName, domains []QualifiedDomainName, joinFn JoinFn, handlerFn Handler, opts ...ConsumerOption) (*WorkPool, <-chan Cluster) {
	quit := iox.NewAsyncCloser()
	p := &WorkPool{
		AsyncCloser: quit,
		cl:          cl,
		self:        consumer,
		service:     service,
		domains:     domains,
		joinFn:      joinFn,
		handler:     handlerFn,
		opts:        opts,
		cluster:     NewClusterMap(NewClusterID(consumer.Instance(), cl.Now())), // empty self-origin map
		clusters:    make(chan Cluster, 1),
		grants:      map[GrantID]*grant{},
		shards:      map[Shard]GrantID{},
		expire:      make(chan bool, 1),
		inject:      make(chan func()),
		quit:        quit,
		drain:       iox.WithQuit(quit.Closed(), iox.NewAsyncCloser()),
	}

	go p.join(context.Background())
	go p.process(context.Background())
	return p, p.clusters
}

func (p *WorkPool) Drain(timeout time.Duration) {
	p.drain.Close()
	p.cl.AfterFunc(timeout, p.Close)
}

// Drained returns a channel that is closed when the pool is drained.
func (p *WorkPool) Drained() <-chan struct{} {
	return p.drain.Closed()
}

func (p *WorkPool) join(ctx context.Context) {
	wctx, _ := contextx.WithQuitCancel(ctx, p.Closed())

	for !p.drain.IsClosed() {
		// Not connected. Establish WorkPool with coordinator with blocking join call. The workpool is either
		// connecting or connected while in the join call.

		log.Infof(ctx, "WorkPool %v attempting to join coordinator", p.self)

		err := p.joinFn(wctx, p.self.Instance(), func(ctx context.Context, in <-chan ConsumerMessage) (<-chan ConsumerMessage, error) {
			return syncx.Txn1(ctx, p.txn, func() (<-chan ConsumerMessage, error) {
				return p.joinCoordinator(ctx, in)
			})
		})

		log.Infof(ctx, "WorkPool %v disconnected from coordinator: %v", p.self, err)

		syncx.Txn0(wctx, p.txn, func() {
			p.lostCoordinator(ctx)
		})

		p.cl.Sleep(time.Second + randx.Duration(time.Second)) // short random backoff on disconnect
	}
}

func (p *WorkPool) joinCoordinator(ctx context.Context, in <-chan ConsumerMessage) (<-chan ConsumerMessage, error) {
	p.lostCoordinator(ctx) // sanity check

	active := mapx.MapValuesIf(p.grants, func(g *grant) (Grant, bool) {
		return g.ToUpdated(), g.LeaseState == LeaseStale
	})

	log.Debugf(ctx, "Connected to coordinator, #grants=%v, #active=%v", len(p.grants), len(active))

	out := make(chan ConsumerMessage, 2_000)
	out <- NewRegister(p.self, p.service, p.domains, active, p.opts...)

	p.in = in
	p.out = out
	p.status = &joinStatus{
		connected: p.cl.Now(),
	}
	p.lease = nil

	return out, nil
}

func (p *WorkPool) lostCoordinator(ctx context.Context) {
	if p.status == nil {
		return // ok: already disconnected
	}

	log.Infof(ctx, "Lost connection to coordinator, connected=%v, #grants=%v", p.cl.Since(p.status.connected), len(p.grants))

	close(p.out)

	p.in = make(chan ConsumerMessage)
	p.out = make(chan ConsumerMessage)
	p.status = nil
	p.lease = nil

	for _, g := range p.grants {
		if g.LeaseState != LeaseRevoked {
			g.LeaseState = LeaseStale
		}
	}
}

func (p *WorkPool) process(ctx context.Context) {
	defer p.Close()

	statsTimer := p.cl.NewTicker(statsDuration)
	defer statsTimer.Stop()

steady:
	for {
		select {
		case msg, ok := <-p.in:
			if !ok {
				log.Errorf(ctx, "Coordinator connection unexpectedly closed")
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
			log.Debugf(ctx, "WorkPool %v, joined=%v, #shards=%v, #grants=%v", p.self, p.status != nil, len(p.shards), len(p.grants))
			p.emitMetrics(ctx)

		case <-p.drain.Closed():
			break steady

		case <-p.Closed():
			return
		}
	}

	log.Infof(ctx, "WorkPool %v draining, joined=%v, #grants=%v", p.self, p.status != nil, len(p.grants))
	now := p.cl.Now()

	if p.status == nil || !p.trySend(ctx, NewDeregister()) {
		log.Errorf(ctx, "WorkPool %v draining while disconnected/stuck. Hard close", p.self)
		return
	}

	for {
		select {
		case msg, ok := <-p.in:
			if !ok {
				if len(p.grants) == 0 {
					log.Infof(ctx, "Workpool %v drained after %v", p.self, p.cl.Since(now))
					p.lostCoordinator(ctx)
					return
				}
				log.Errorf(ctx, "Coordinator connection unexpectedly closed while draining. Hard close")
				p.lostCoordinator(ctx)
				return
			}
			p.handleMessage(ctx, msg)

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
					old.LeaseState = LeaseActive
					old.Lease = p.lease

					log.Debugf(ctx, "Re-activating stale grant %v", old)
					continue
				} else {
					log.Errorf(ctx, "Internal: unexpected re-grant of non-stale grant %v. Re-creating", old)

					old.Handler.Close()
					delete(p.grants, g.ID())
				}
			}

			h := newHandler(ctx, p.cl, g, p.lease.Ttl(), newLoader(), newUnloader(), p.handler)
			p.grants[g.ID()] = &grant{
				Grant:      g,
				LeaseState: LeaseActive,
				Lease:      p.lease,
				Handler:    h,
			}

			// Wait on client Load
			go func() {
				select {
				case <-h.own.loader.loaded().Closed():
					syncx.Txn0(ctx, p.txn, func() {
						if grant, ok := p.grants[g.ID()]; ok {
							if grant.Grant.State() == AllocatedGrantState {
								grant.Grant = grant.ToState(LoadedGrantState)
								log.Debugf(ctx, "Informing the coordinator of Grant Load %v", grant.Grant)
								p.mustSend(ctx, NewUpdate(grant.Grant))
							}
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
				case <-h.own.unloader.unloaded().Closed(): // client closed ownership.Unloader.Unload()
					syncx.Txn0(ctx, p.txn, func() {
						if grant, ok := p.grants[g.ID()]; ok {
							if grant.Grant.State() == RevokedGrantState {
								grant.Grant = grant.ToState(UnloadedGrantState)
								log.Debugf(ctx, "Informing the coordinator of Grant Unload %v", grant.Grant)
								p.mustSend(ctx, NewUpdate(grant.Grant))
							}
						}
					})
					return
				case <-h.Closed():
					return
				}
			}()

			go func() {
				<-h.Closed()

				syncx.Txn0(ctx, p.txn, func() {
					p.removeGrant(ctx, g.ID())
				})
			}()

			log.Debugf(ctx, "Created handler for grant %v", g)
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
			candidate.Grant = g // Overwrite allocated grant with active grant
			log.Debugf(ctx, "Promoted grant to active: %v", g)
			candidate.Handler.own.activate() // Signal consumer that grant is activated
		}

	case msg.IsRevoke():
		revoke, _ := msg.Revoke()
		grants := revoke.Grants()
		leases := map[time.Time]*clockx.Timer{}

		for _, g := range grants {
			grant, ok := p.grants[g.ID()]
			if !ok {
				log.Warnf(ctx, "Revoking stale grant: %v. Ignoring", g)
				continue
			}

			// (1) Create lease that expires at the current TTL. The workpool lease no longer includes this grant.
			// We share the leases to not get a flood of identical expiration checks.

			ttl := grant.Lease.Ttl()
			if _, ok := leases[ttl]; !ok {
				leases[ttl] = clockx.AfterFunc(p.cl, p.cl.Until(ttl), p.emitExpirationCheck)
			}
			grant.Grant = g // Overwrite activated grant with revoked grant
			grant.LeaseState = LeaseRevoked
			grant.Lease = leases[ttl]
			grant.Handler.own.revoke() // Signal consumer that grant is revoked

			// (2) Revoke async. If the Revoke unexpectedly arrives before Assign, the lease still expires.

			log.Infof(ctx, "Revoking grant %v, ttl=%v", g, ttl)

			grant.Handler.Drain(p.cl.Until(ttl))
		}

	case msg.IsNotify():
		notify, _ := msg.Notify()
		update, target := notify.Update(), notify.Target()

		log.Debugf(ctx, "Received grant update %v, for target %v", update, target)

		g, ok := p.grants[target.ID()]
		if !ok {
			log.Warnf(ctx, "Updating stale grant: %v. Ignoring", target)
			break
		}

		switch update.State() {
		case LoadedGrantState:
			g.Handler.own.unloader.load() // Notify client that grant was loaded
		case UnloadedGrantState:
			g.Handler.own.loader.unload() // Notify client that grant was unloaded
		}

	case msg.IsExtend():
		extend, _ := msg.Extend()

		if p.lease == nil {
			p.lease = clockx.AfterFunc(p.cl, p.cl.Until(extend.Lease()), p.emitExpirationCheck)
		}
		p.lease.Reset(p.cl.Until(extend.Lease()))

		// Update informational consumer lease
		for _, grant := range p.grants {
			grant.Handler.own.setExpiration(extend.Lease())
		}

	default:
		log.Errorf(ctx, "Unexpected coordinator message: %v", msg)
	}
}

func (p *WorkPool) handleClusterMessage(ctx context.Context, msg ClusterMessage) {
	// Cluster update. Merge with current cluster and forward to listeners.

	upd, err := UpdateClusterMap(p.cluster, msg)
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

	now := p.cl.Now()
	for gid, g := range p.grants {
		if g.Lease.Ttl().Before(now) {
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

	if grant.LeaseState != LeaseStale && p.cl.Now().Before(grant.Lease.Ttl()) {
		p.mustSend(ctx, NewReleased(grant.Grant))
	}
	delete(p.grants, gid)
	if p.shards[grant.Grant.Shard()] == gid {
		delete(p.shards, grant.Grant.Shard()) // delete service tracking if not replaced by new grant
	}

	log.Debugf(ctx, "WorkPool %v removed grant %v", p.self, grant)
}

func (p *WorkPool) emitMetrics(ctx context.Context) {
	p.resetMetrics(ctx)

	grants := map[QualifiedDomainName]map[LeaseState]int{}
	for _, grant := range p.grants {
		service := grant.Grant.Shard().Domain
		if grants[service] == nil {
			grants[service] = map[LeaseState]int{}
		}
		grants[service][grant.LeaseState] += 1
	}

	for domain, counts := range grants {
		for state, count := range counts {
			numGrants.Set(ctx, float64(count), slicex.CopyAppend(qualifiedDomainTags(domain), leaseStateTag(string(state)))...)
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

	timer := p.cl.NewTimer(100 * time.Millisecond)
	defer timer.Stop()

	select {
	case p.out <- msg:
		return
	case <-timer.C:
		log.Errorf(ctx, "Internal: coordinator WorkPool stuck. Disconnecting")
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
