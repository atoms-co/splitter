package worker

import (
	"context"
	"atoms.co/lib-go/pkg/clock"
	"go.atoms.co/splitter/lib/service/session"
	"go.atoms.co/lib/log"
	"go.atoms.co/lib/contextx"
	"go.atoms.co/lib/net/grpcx"
	"go.atoms.co/lib/iox"
	"go.atoms.co/lib/mapx"
	"go.atoms.co/lib/randx"
	"go.atoms.co/lib/syncx"
	"go.atoms.co/splitter/pkg/core"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pkg/service/coordinator"
	"go.atoms.co/splitter/pkg/service/leader"
	"go.atoms.co/splitter/pkg/util/txnx"
	"fmt"
	"time"
)

const (
	statsDuration = 15 * time.Second
)

type JoinFn func(ctx context.Context, handler grpcx.Handler[leader.WorkerMessage, leader.WorkerMessage]) error

type CoordinatorFactory func(ctx context.Context, service model.QualifiedServiceName, state core.State) coordinator.Coordinator

type joinStatus struct {
	connected time.Time
}

type Worker struct {
	iox.AsyncCloser

	cl      clock.Clock
	self    model.Instance
	joinFn  JoinFn
	factory CoordinatorFactory

	status *joinStatus                 // leader connectivity status
	in     <-chan leader.WorkerMessage // leader incoming messages (empty and not closed, if disconnected)
	out    chan<- leader.WorkerMessage // leader outgoing messages (empty and not closed, if disconnected)

	grants map[core.GrantID]*Grant
	lease  *RenewableLease

	expire chan bool
	inject chan func()

	drain iox.AsyncCloser
}

func New(cl clock.Clock, self model.Instance, joinFn JoinFn, factory CoordinatorFactory) *Worker {
	w := &Worker{
		AsyncCloser: iox.NewAsyncCloser(),
		cl:          cl,
		self:        self,
		joinFn:      joinFn,
		factory:     factory,
		in:          make(chan leader.WorkerMessage),
		out:         make(chan leader.WorkerMessage),
		grants:      make(map[core.GrantID]*Grant),
		expire:      make(chan bool, 1),
		inject:      make(chan func()),
		drain:       iox.NewAsyncCloser(),
	}
	go w.join(context.Background())
	go w.process(context.Background())
	return w
}

// Connect handles connection of a consumer to a local coordinator
func (w *Worker) Connect(ctx context.Context, sid session.ID, gid core.GrantID, register model.RegisterMessage, in <-chan model.ConsumerMessage) (<-chan model.ConsumerMessage, error) {
	tenant := register.TenantName()

	c, err := syncx.Txn1(ctx, txnx.Txn(w, w.inject), func() (coordinator.Coordinator, error) {
		g, ok := w.grants[gid]
		if !ok {
			return nil, fmt.Errorf("%w: grant %v not found for tenant %v", model.ErrNotFound, gid, tenant)
		}
		return g.Coordinator, nil
	})
	if err != nil {
		return nil, err
	}
	return c.Connect(ctx, sid, register, in)
}

func (w *Worker) Drain(timeout time.Duration) {
	w.drain.Close()
	w.cl.AfterFunc(timeout, w.Close)
}

func (w *Worker) join(ctx context.Context) {
	wctx, _ := contextx.WithQuitCancel(ctx, w.drain.Closed())

	for !w.drain.IsClosed() {
		// Not connected. Establish connection with leader with blocking join call. The worker is either
		// connecting or connected while in the join call.

		log.Infof(ctx, "Worker %v attempting to join leader", w.self)

		err := w.joinFn(wctx, func(ctx context.Context, in <-chan leader.WorkerMessage) (<-chan leader.WorkerMessage, error) {
			return syncx.Txn1(ctx, txnx.Txn(w, w.inject), func() (<-chan leader.WorkerMessage, error) {
				return w.joinLeader(ctx, in)
			})
		})

		log.Infof(ctx, "Worker %v disconnected from leader: %v", w.self, err)

		syncx.Txn0(wctx, txnx.Txn(w, w.inject), func() {
			w.lostLeader(ctx)
		})

		w.cl.Sleep(time.Second + randx.Duration(time.Second)) // short random backoff on disconnect
	}
}

func (w *Worker) joinLeader(ctx context.Context, in <-chan leader.WorkerMessage) (<-chan leader.WorkerMessage, error) {
	w.lostLeader(ctx) // sanity check

	active := mapx.MapValuesIf(w.grants, func(g *Grant) (core.Grant, bool) {
		return g.ToUpdated(), g.State == GrantStale
	})

	log.Debugf(ctx, "Connected to leader, #grants=%v, #active=%v", len(w.grants), len(active))

	out := make(chan leader.WorkerMessage, 2_000)
	out <- leader.NewRegister(w.self, active...)

	w.in = in
	w.out = out
	w.status = &joinStatus{
		connected: w.cl.Now(),
	}
	w.lease = nil

	return out, nil
}

func (w *Worker) lostLeader(ctx context.Context) {
	if w.status == nil {
		return // ok: already disconnected
	}

	log.Infof(ctx, "Lost connection to leader, connected=%v, #grants=%v", w.cl.Since(w.status.connected), len(w.grants))

	close(w.out)

	w.in = make(chan leader.WorkerMessage)
	w.out = make(chan leader.WorkerMessage)
	w.status = nil
	w.lease = nil

	for _, g := range w.grants {
		if g.State != GrantRevoked {
			g.State = GrantStale
		}
	}
}

func (w *Worker) process(ctx context.Context) {
	wctx, _ := contextx.WithQuitCancel(ctx, w.Closed())
	defer w.Close()

	statsTimer := w.cl.NewTicker(statsDuration)
	defer statsTimer.Stop()

steady:
	for {
		select {
		case msg, ok := <-w.in:
			if !ok {
				return
			}
			w.handleWorkerMessage(wctx, msg)

		case <-w.expire:
			w.checkExpiration(wctx)

		case fn := <-w.inject:
			fn()

		case <-statsTimer.C:
			// record metrics

		case <-w.drain.Closed():
			break steady

		case <-w.Closed():
			return
		}
	}

	log.Infof(ctx, "Worker %v draining, joined=%v, #grants=%v", w.self, w.status != nil, len(w.grants))
	now := w.cl.Now()

	if w.status == nil || !w.trySend(ctx, leader.NewDeregister()) {
		log.Errorf(ctx, "Worker %v draining while disconnected/stuck. Hard close", w.self)
		return
	}

	for {
		if len(w.grants) == 0 {
			log.Infof(ctx, "Worked %v drained after %v", w.self, w.cl.Since(now))
			return
		}

		select {
		case msg, ok := <-w.in:
			if !ok {
				log.Errorf(wctx, "Leader connection unexpectedly closed while draining. Hard close")
				w.lostLeader(ctx)
				return
			}
			w.handleWorkerMessage(wctx, msg)

		case <-w.expire:
			w.checkExpiration(wctx)

		case fn := <-w.inject:
			fn()

		case <-w.Closed():
			return
		}
	}
}

func (w *Worker) handleWorkerMessage(ctx context.Context, msg leader.WorkerMessage) {
	switch {
	case msg.IsAssign():
		assign, _ := msg.Assign()
		grant, state := assign.Grant(), assign.State()

		if old, ok := w.grants[grant.ID()]; ok {
			if old.State == GrantStale {
				old.State = GrantActive
				old.Lease = w.lease

				log.Debugf(ctx, "Re-activating stale grant %v", old)
				return
			} else {
				log.Errorf(ctx, "Internal: unexpected re-grant of non-stale grant %v. Re-creating", old)

				old.Coordinator.Close()
				delete(w.grants, grant.ID())
			}
		} else {
			// Grant not present, check for conflicting grants

			for _, old := range w.grants {
				if old.Grant.Service() == grant.Service() {
					log.Warnf(ctx, "Internal: grant %v replaces existing grant: %v", grant, old)
				}
			}
		}

		w.grants[grant.ID()] = &Grant{
			Grant: grant,
			State: GrantActive,
			Lease: w.lease,
		}

		c := w.factory(ctx, grant.Service(), state)
		w.grants[grant.ID()].Coordinator = c // TODO(jhhurwitz) 10/31/23 Consider async Coordinator creation

		go func() {
			<-c.Closed()
			syncx.Txn0(ctx, txnx.Txn(w, w.inject), func() {
				w.removeGrant(ctx, grant.ID())
			})
		}()

		log.Debugf(ctx, "Created coordinator %v for grant %v", c, grant)

	case msg.IsRevoke():
		revoke, _ := msg.Revoke()
		grants := revoke.Grants()
		leases := map[time.Time]Lease{}

		for _, g := range grants {
			gid := g.ID()
			grant, ok := w.grants[gid]
			if !ok {
				log.Warnf(ctx, "Revoking stale grant: %v. Ignoring", g)
				continue
			}

			// (1) Create lease that expires at the current TTL. The worker lease no longer includes this grant.
			// We share the leases to not get a flood of identical expiration checks.

			ttl := grant.Lease.Expiration()
			if _, ok := leases[ttl]; !ok {
				leases[ttl] = NewRenewableLease(ttl, w.cl.AfterFunc(w.cl.Until(ttl), w.emitExpirationCheck))
			}
			grant.State = GrantRevoked
			grant.Lease = leases[ttl]

			// (2) Revoke async. If the Revoke unexpectedly arrives before Assign, the lease still expires.

			log.Infof(ctx, "Revoking grant %v, ttl=%v", g, ttl)

			grant.Coordinator.Drain(w.cl.Until(ttl))
		}

	case msg.IsDisconnect():
		log.Infof(ctx, "Leader requested disconnect for worker %v", w.self)

		// TODO(jhhurwitz) 10/32/2023: add a closer that will cancel the the joinFn context
		w.lostLeader(ctx)

	case msg.IsLeaseUpdate():
		lease, _ := msg.LeaseUpdate()

		if w.lease == nil {
			w.lease = NewRenewableLease(lease.Ttl(), nil)
		}
		w.lease.Renew(lease.Ttl(), w.cl.AfterFunc(w.cl.Until(lease.Ttl()), w.emitExpirationCheck))

	default:
		log.Errorf(ctx, "Invalid worker message: %v", msg)
	}
}

func (w *Worker) emitExpirationCheck() {
	select {
	case <-w.expire:
	default:
	}
	w.expire <- true
}

func (w *Worker) checkExpiration(ctx context.Context) {
	// Possible grant expiration

	now := w.cl.Now()
	for gid, g := range w.grants {
		if g.Lease.Expiration().Before(now) {
			w.removeGrant(ctx, gid)
		}
	}
}

func (w *Worker) removeGrant(ctx context.Context, gid core.GrantID) {
	grant, ok := w.grants[gid]
	if !ok {
		return // ok: no longer present
	}
	grant.Coordinator.Close()

	if grant.State != GrantStale && w.cl.Now().Before(grant.Lease.Expiration()) {
		w.mustSend(ctx, leader.NewRelinquished(grant.Grant))
	}

	delete(w.grants, gid)

	log.Debugf(ctx, "Worker %v removed grant %v", w.self, grant)
}

func (w *Worker) trySend(ctx context.Context, msg leader.WorkerMessage) bool {
	select {
	case w.out <- msg:
		return true
	default:
		return false
	}
}

func (w *Worker) mustSend(ctx context.Context, msg leader.WorkerMessage) {
	if w.status == nil {
		return // ok: disconnected
	}

	timer := w.cl.NewTimer(100 * time.Millisecond)
	defer timer.Stop()

	select {
	case w.out <- msg:
		return
	case <-timer.C:
		log.Errorf(ctx, "Internal: leader connection stuck. Disconnecting")
		w.lostLeader(ctx)
	}
}
