package leader

import (
	"context"
	"atoms.co/lib-go/pkg/clock"
	"go.atoms.co/splitter/lib/service/location"
	"go.atoms.co/splitter/lib/service/session"
	"go.atoms.co/lib/log"
	"go.atoms.co/lib/metrics"
	"go.atoms.co/lib/chanx"
	"go.atoms.co/lib/contextx"
	"go.atoms.co/lib/iox"
	"go.atoms.co/lib/mapx"
	"go.atoms.co/lib/randx"
	"go.atoms.co/slicex"
	"go.atoms.co/lib/syncx"
	"go.atoms.co/splitter/pkg/allocation"
	"go.atoms.co/splitter/pkg/core"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pkg/storage"
	"go.atoms.co/splitter/pkg/util/sessionx"
	"go.atoms.co/splitter/pb/private"
	"go.atoms.co/splitter/pb"
	"fmt"
	"sync"
	"time"
)

const (
	// handleTimeout is the timeout for handle requests.
	handleTimeout = 5 * time.Second
	// registrationTimeout is the timeout for observing the initial register request from a worker connection.
	registrationTimeout = 10 * time.Second
	// leaseDuration is the granted lease duration for tenants. It should be long enough to accommodate a reconnect,
	// but not so long tenants are idle for too long if the connection has restarted.
	leaseDuration = 20 * time.Second
)

var (
	numWorkers  = metrics.NewTrackedGauge(metrics.NewGauge("go.atoms.co/splitter/leader_workers", "Connected worker status", core.StatusKey))
	numServices = metrics.NewTrackedGauge(metrics.NewGauge("go.atoms.co/splitter/leader_services", "Service count", core.TenantKey))

	numActions = metrics.NewCounter("go.atoms.co/splitter/leader_actions", "Leader actions", core.ActionKey, core.ResultKey)
)

type Option func(*Leader)

func WithFastActivation() Option {
	return func(leader *Leader) {
		leader.fastActivation = true
	}
}

// Leader centralizes tenant and storage coordination. All updates go through the global leader, which is
// dynamically selected and may be present at different nodes at different times.
type Leader struct {
	iox.AsyncCloser

	cl clock.Clock
	id location.Instance

	// options
	fastActivation bool

	writer *Writer
	cache  *storage.Cache // post-commit cache

	workers  map[location.InstanceID]*workerSession
	alloc    *Allocation
	messages chan *sessionx.Message[Message]

	upd    <-chan core.Update
	del    <-chan core.Delete
	res    <-chan core.Restore
	inject chan func()

	initialized, drain iox.AsyncCloser
}

func New(ctx context.Context, cl clock.Clock, loc location.Location, db storage.Storage, opts ...Option) *Leader {
	writer, upd, del, res := NewWriter(cl, db)

	l := &Leader{
		AsyncCloser: iox.NewAsyncCloser(),
		cl:          cl,
		id:          location.NewInstance(loc, location.WithName("leader")),
		writer:      writer,
		cache:       storage.NewCache(),
		workers:     map[model.InstanceID]*workerSession{},
		messages:    make(chan *sessionx.Message[Message], 1000),
		upd:         upd,
		del:         del,
		res:         res,
		inject:      make(chan func()),
		initialized: iox.NewAsyncCloser(),
		drain:       iox.WithQuit(ctx.Done(), iox.NewAsyncCloser()), // context cancel => drain
	}
	for _, opt := range opts {
		opt(l)
	}

	go l.init(context.Background(), cl.Now())

	return l
}

func (l *Leader) Join(ctx context.Context, sid session.ID, in <-chan Message) (<-chan Message, error) {
	msg, ok := chanx.TryRead(in, registrationTimeout)
	if !ok {
		log.Errorf(ctx, "No registration message received")
		return nil, model.WrapError(fmt.Errorf("no registration message: %w", model.ErrInvalid))
	}

	workerMsg, ok := msg.WorkerMessage()
	if !ok || !workerMsg.IsRegister() {
		log.Errorf(ctx, "expected registration message, got %v", workerMsg)
		return nil, model.WrapError(fmt.Errorf("invalid registration message: %w", model.ErrInvalid))
	}
	register, _ := workerMsg.Register()

	log.Infof(ctx, "Registration for %v: %v, #grants=%v", sid, register.Worker(), len(register.Active()))

	return syncx.Txn1(ctx, l.txn, func() (<-chan Message, error) {
		wctx, cancel := contextx.WithQuitCancel(context.Background(), l.Closed())

		now := l.cl.Now()
		s, out := l.connect(wctx, now, sid, register, in)
		l.allocate(ctx, now, false) // Allocate to assign work post connection

		go func() {
			defer cancel()
			<-s.connection.Closed()

			syncx.AsyncTxn(l.txn, func() {
				cur, ok := l.workers[s.instance.ID()]
				if ok && cur.connection.Sid() == sid { // guard against race
					l.disconnect(wctx, "connection closed", cur)
				}
				s.connection.Disconnect() // Close channel
			})
		}()
		return out, nil
	})
}

func (l *Leader) Handle(ctx context.Context, req HandleRequest) (*internal_v1.LeaderHandleResponse, error) {
	wctx, cancel := context.WithTimeout(ctx, handleTimeout)
	defer cancel()

	resp, err := l.handle(wctx, req)
	if err != nil {
		log.Debugf(ctx, "Leader %v request %v failed: %v", l.id, req, err)
		return nil, model.WrapError(err)
	}
	return resp, nil
}

func (l *Leader) Initialized() iox.AsyncCloser {
	return l.initialized
}

func (l *Leader) String() string {
	return l.id.String()
}

func (l *Leader) init(ctx context.Context, now time.Time) {
	defer l.Close()
	defer l.drain.Close()
	defer l.initialized.Close()
	defer l.writer.Close()

	log.Infof(ctx, "Leader initializing: %v", l.id)

	snapshot, err := l.writer.Init(ctx)
	if err != nil {
		log.Errorf(ctx, "Failed to load tenant data: %v", err)
		recordAction(ctx, "init", "failed")
		return
	}
	l.cache.Restore(snapshot)

	delay := leaseDuration
	if l.fastActivation {
		delay = 0
	}
	log.Infof(ctx, "Activating at %v", now.Add(delay))
	l.alloc = NewAllocation(l.id.ID(), snapshot, now.Add(delay))

	if l.drain.IsClosed() {
		log.Errorf(ctx, "Unexpected: leader %v lost leadership while loading", l.id)
		recordAction(ctx, "init", "aborted")
		return
	}

	log.Infof(ctx, "Leader initialized: %v, #tenants=%v", l.id, len(snapshot.Tenants()))
	recordAction(ctx, "init", "ok")
	l.initialized.Close()

	l.process(ctx)

	// Close worker connections
	for _, w := range l.workers {
		w.TrySend(ctx, NewDisconnect())
		w.connection.Disconnect()
	}

	log.Infof(ctx, "Leader exited: %v", l.id)
}

// TODO(herohde) 9/2/2013: unclear how leader election transitions. Immediately? If we want a quick drain
// to refresh lease updates etc, we may want need a matching startup delay to guarantee exclusivity.

func (l *Leader) process(ctx context.Context) {
	defer l.resetMetrics(ctx)

	ticker := l.cl.NewTicker(4*time.Second + randx.Duration(time.Second))
	defer ticker.Stop()

	slow := l.cl.NewTicker(time.Minute + randx.Duration(10*time.Second))
	defer slow.Stop()

steady:
	for {
		select {
		case m := <-l.messages:
			w, ok := l.workers[m.Instance.ID()]
			if !ok || w.connection.Sid() != m.Sid {
				log.Debugf(ctx, "Ignoring stale message %v: %v", m.Instance, m.Msg)
				break
			}
			l.handleMessage(ctx, w, m.Msg)

		case <-ticker.C:
			// Regularly send out lease updates to healthy workers, disconnect unhealthy ones.

			now := l.cl.Now()
			lease := now.Add(leaseDuration)

			var unhealthy []*workerSession
			for _, w := range l.workers {
				if w.draining {
					continue // skip: don't extend lease if draining
				}
				if !w.TrySend(ctx, NewLeaseUpdate(lease)) {
					unhealthy = append(unhealthy, w)
					continue
				}
				l.alloc.Extend(w.instance.ID(), lease)
			}
			if len(unhealthy) > 0 {
				log.Warnf(ctx, "Disconnecting  %v unhealthy workers: %v", len(unhealthy), unhealthy)
			}
			l.disconnect(ctx, "unhealthy", unhealthy...)
			l.allocate(ctx, now, true)

			l.emitMetrics(ctx)

		case <-slow.C:
			// Move placements by N blocks every 5 minutes, if needed.

			cutoff := l.cl.Now().Add(-5 * time.Minute)

			for _, t := range l.cache.Tenants() {
				for _, info := range l.cache.Placements(t.Name()) {
					placement := info.InternalPlacement()

					if !placement.IsActive() || !info.Timestamp().Before(cutoff) {
						continue // skip: not active or too recently changed
					}
					target, current, n := placement.Target(), placement.Current(), placement.BlocksPerCycle()
					if target.Equals(current) {
						continue // skip: on target
					}

					moved := core.MoveBlockDistribution(current, target, n)
					cfg := core.NewInternalPlacementConfig(target, moved, n)
					upd := core.NewInternalPlacement(placement.Name(), cfg, l.cl.Now())

					if err := l.writer.UpdatePlacementAsync(ctx, upd, info.Version()); err != nil {
						log.Errorf(ctx, "Failed to move dynamic region placement %v: %v", placement.Name(), err)
					} else {
						log.Infof(ctx, "Updated dynamic region placement %v: %v -> %v, target=%v", placement.Name(), current, moved, target)
					}
				}
			}

		case fn := <-l.inject:
			fn()

		case upd := <-l.upd:
			// State update. Update set of coordinators and update existing coordinator state.

			if err := l.cache.Update(upd, false); err != nil {
				log.Errorf(ctx, "Internal: inconsistent tenant state: %v", err)
				return
			}

			l.refresh(ctx)
			l.update(ctx, upd)
			l.allocate(ctx, l.cl.Now(), false)

		case del := <-l.del:
			// Tenant deletion. Remove impacted services. No need for allocation.

			if err := l.cache.Delete(del); err != nil {
				log.Errorf(ctx, "Internal: inconsistent tenant state: %v", err)
				return
			}

			l.refresh(ctx)

		case res := <-l.res:
			// Full state restoration. Disconnect all workers to force re-grant.

			l.cache.Restore(res.Snapshot())

			l.disconnect(ctx, "state reset", mapx.Values(l.workers)...)
			l.refresh(ctx)
			l.allocate(ctx, l.cl.Now(), false)

		case <-l.drain.Closed():
			break steady

		case <-l.writer.Closed():
			log.Errorf(ctx, "Writer closed. Exiting")
			return

		case <-l.Closed():
			return
		}
	}

	log.Infof(ctx, "Leader %v draining, #workers=%v", l.id, len(l.workers))
}

func (l *Leader) handleMessage(ctx context.Context, w *workerSession, m Message) {
	msg, ok := m.WorkerMessage()
	if !ok {
		log.Errorf(ctx, "Internal: unexpected message: %v", m)
		l.disconnect(ctx, "invalid message", w)
		return
	}

	switch {
	case msg.IsDeregister():
		deregister, _ := msg.Deregister()
		l.handleDeregister(ctx, w, deregister)

	case msg.IsRelinquished():
		relinquished, _ := msg.Relinquished()
		l.handleRelinquished(ctx, w, relinquished)

	default:
		log.Errorf(ctx, "Internal: unexpected worker message: %v", msg)
		l.disconnect(ctx, "invalid message", w)
	}
}

func (l *Leader) handleDeregister(ctx context.Context, w *workerSession, deregister DeregisterMessage) {
	log.Infof(ctx, "Received de-register from worker %v", w)

	// (1) Mark worker as suspended to prevent new work.

	w.draining = true
	l.alloc.Suspend(w.instance.ID())

	// (2) Revoke all active grants. The leader sends out new assignments only on Active grants,
	// so Allocated grants can just be released. Revoked assignments are already in progress.

	now := l.cl.Now()

	assigned := l.alloc.Assigned(w.instance.ID())
	for _, g := range assigned.Allocated {
		l.alloc.Release(g, now) // no promotion
	}
	if len(assigned.Active) > 0 {
		l.alloc.Revoke(w.instance.ID(), now, assigned.Active...)

		if !w.TrySend(ctx, NewRevoke(slicex.Map(assigned.Active, toGrant)...)) {
			log.Errorf(ctx, "Failed to revoke %v grants for worker: %v. Disconnecting", len(assigned.Active), w)
			l.disconnect(ctx, "stuck", w)
			return
		}
	}

	// (3) If no grants, disconnect immediately. Otherwise, wait for last release or expiration.

	if assigned.IsEmpty() {
		l.disconnect(ctx, "no grant deregister", w) // no grants, safe to disconnect
		return
	}

	log.Infof(ctx, "Deregistered worker %v with %v active grants", w, len(assigned.Active))
}

func (l *Leader) handleRelinquished(ctx context.Context, w *workerSession, relinquished RelinquishedMessage) {
	now := l.cl.Now()

	// (1) Release relinquished grants. Check deregister status

	var promoted []Grant
	for _, g := range relinquished.Grants() {
		grant := fromGrant(w.instance.ID(), g)

		if promo, ok := l.alloc.Release(grant, now); ok {
			promoted = append(promoted, promo)
		}
	}

	if w.draining && l.alloc.Assigned(w.instance.ID()).IsEmpty() {
		l.disconnect(ctx, "complete deregister", w)
	}

	// (2) Assign promoted, if any

	l.assign(ctx, now, promoted...)
}

func (l *Leader) connect(ctx context.Context, now time.Time, sid session.ID, register RegisterMessage, in <-chan Message) (*workerSession, <-chan Message) {
	worker := register.Worker()
	var active []Grant
	for _, g := range register.Active() {
		active = append(active, fromGrant(worker.ID(), g))
	}

	if old, ok := l.workers[worker.ID()]; ok {
		log.Infof(ctx, "Worker %v re-connected (session=%v) with #grants: %d. Disconnecting stale session", worker, sid, len(active))
		l.disconnect(ctx, "reconnect", old)
	} else {
		log.Infof(ctx, "Worker %v connected (session=%v) with #grants: %d", worker, sid, len(active))
	}

	// TODO(jhhurwitz): 12/06/23: Custom size
	connection, out := sessionx.NewConnection[Message](l.cl, sid, worker, l, in, l.messages)
	w := &workerSession{
		instance:   worker,
		connection: connection,
	}
	l.workers[worker.ID()] = w

	lease := l.cl.Now().Add(leaseDuration)
	w.TrySend(ctx, NewLeaseUpdate(lease)) // grants will be covered under this lease

	// TODO(jhhurwitz): 12/06/23: Claim grants
	if assigned, ok := l.alloc.Attach(allocation.NewWorker(w.instance.Instance().ID(), w.instance), lease, active...); ok {
		for _, grant := range assigned.Active {
			tenant := grant.Unit.Tenant
			state, _ := l.cache.State(tenant)

			if !w.TrySend(ctx, NewAssign(toGrant(grant), state)) {
				// TODO(herohde) 11/4/2023: The buffer is too small. We don't want that to happen. Revoke for
				// now to avoid re-connect death loop.

				log.Errorf(ctx, "Internal: failed to send regrant %v to worker: %v. Revoking", grant, w)
				l.alloc.Revoke(w.instance.ID(), now, grant)
			}
		}
		// NOTE(herohde) 11/4/2023: Ignore Allocated and let Revoked expire.
	}

	if !w.TrySend(ctx, NewClusterSnapshot(l.snapshot())) {
		log.Errorf(ctx, "Internal: failed to send initial cluster map to worker: %v. Closing", w)
		connection.Disconnect()
	}

	return w, out
}

func (l *Leader) disconnect(ctx context.Context, reason string, workers ...*workerSession) {
	for _, w := range workers {
		log.Infof(ctx, "Disconnecting worker (reason: %v): %v", reason, w)
		recordAction(ctx, "disconnect", "ok")

		now := l.cl.Now()

		if l.alloc.Detach(w.instance.ID()) {
			assigned := l.alloc.Assigned(w.instance.ID())
			for _, g := range assigned.Allocated {
				l.alloc.Release(g, now) // no promotion
			}
			if len(assigned.Active) > 0 {
				log.Warnf(ctx, "Detached worker %v with active services: %v ", w, assigned.Active)
			}
		} // else: already detached

		w.TrySend(ctx, NewDisconnect()) // TODO(herohde) 11/12/2023: needed with session terminated?
		w.connection.Disconnect()
		delete(l.workers, w.instance.ID())
	}
}

// refresh updates the allocation with the services in the latest snapshot.
func (l *Leader) refresh(ctx context.Context) {
	now := l.cl.Now()

	// (1) Update work in allocation and revoke invalid grants. There is no mutations possible for service
	// names, so no need to delay newly created grants.

	upd, rejected := UpdateAllocation(l.alloc, l.cache.Snapshot(), now)
	l.alloc = upd

	l.revoke(ctx, rejected...)

	// (2) If any are due to deleted/decommissioned services, unassign them in the cluster map.

	// ...

	recordAction(ctx, "refresh", "ok")
}

func (l *Leader) revoke(ctx context.Context, grants ...Grant) {
	for wid, list := range byWorker(grants...) {
		w, ok := l.workers[wid]
		if !ok {
			log.Warnf(ctx, "Failed to send revoke to %v for grants: %v", wid, list)
			continue // skip: cannot send revoke, so let lapse
		}

		if !w.TrySend(ctx, NewRevoke(slicex.Map(list, toGrant)...)) {
			log.Errorf(ctx, "Failed to send revoke to worker %v for grants: %v. Disconnecting", w, list)
			l.disconnect(ctx, "stuck", w)
			continue
		}

		recordActions(ctx, len(list), "revoke", "ok")
	}
}

func (l *Leader) allocate(ctx context.Context, now time.Time, loadbalance bool) {
	// (1) Expire, Allocate and LoadBalance. If any worker cannot handle the update
	// they are disconnected. Allocated grants are ignored. If an assignment fails,
	// the grant is immediately released.

	promo := l.alloc.Expire(now)
	grants := l.alloc.Allocate(now)

	if loadbalance {
		if move, load, ok := l.alloc.LoadBalance(now); ok {
			w := l.workers[move.From.Worker]

			if !w.TrySend(ctx, NewRevoke(toGrant(move.From))) {
				log.Errorf(ctx, "Failed to send move %v from worker: %v. Disconnecting", move, w)
				l.disconnect(ctx, "stuck", w)
			} else {
				log.Debugf(ctx, "Initiated grant move: %v, load=%v", move, load)
			}

			recordAction(ctx, "move", "ok")
		}
	}

	l.assign(ctx, now, append(promo, grants...)...)

	log.Debugf(ctx, "Allocation: %v", l.alloc)
}

func (l *Leader) assign(ctx context.Context, now time.Time, grants ...Grant) {
	if len(grants) == 0 {
		return
	}

	// (1) Assign all grant, if possible.

	var assigned []Grant
	for _, grant := range grants {
		if grant.IsAllocated() {
			continue // skip: only active grants are sent
		}

		w, ok := l.workers[grant.Worker]
		if !ok {
			l.alloc.Release(grant, now) // undo assignment
			continue
		}

		tenant := grant.Unit.Tenant
		state, _ := l.cache.State(tenant)

		if !w.TrySend(ctx, NewAssign(toGrant(grant), state)) {
			log.Errorf(ctx, "Failed to send grant %v to worker: %v. Disconnecting", grant, w)

			l.alloc.Release(grant, now) // undo assignment. Safe because it was not sent
			l.disconnect(ctx, "stuck", w)
			recordAction(ctx, "assign", "failed")
			continue
		}
		recordAction(ctx, "assign", "ok")

		log.Infof(ctx, "Assigned new grant to worker %v: %v.", w, tenant)
		assigned = append(assigned, grant)
	}

	// (2) Broadcast incremental cluster updates based on the actual updates made.

	l.broadcast(ctx, assigned...)
}

func (l *Leader) update(ctx context.Context, upd core.Update) {
	for _, w := range l.workers {
		assignments := l.alloc.Assigned(w.instance.ID())
		for _, g := range assignments.Active {
			if g.Unit.Tenant != upd.Name() {
				continue
			}

			// TODO(herohde) 11/15/2023: send just updates relevant to the service.

			if !w.TrySend(ctx, NewUpdate(toGrant(g), upd)) {
				log.Errorf(ctx, "Failed to send state update to worker: %v. Disconnecting", w)
				l.disconnect(ctx, "stuck", w)
				break
			}
		}
	}
}

func (l *Leader) broadcast(ctx context.Context, grants ...Grant) {
	if len(grants) == 0 {
		return
	}

	var assignments []core.Assignment
	for wid, assigned := range byWorker(grants...) {
		info, _ := l.alloc.Worker(wid)

		assignments = append(assignments, core.Assignment{
			Worker: info.Instance.Data,
			Grants: slicex.Map(assigned, toGrant),
		})
	}

	for _, w := range l.workers {
		if !w.TrySend(ctx, NewClusterUpdate(assignments)) {
			log.Errorf(ctx, "Failed to send cluster update to worker: %v. Disconnecting", w)
			l.disconnect(ctx, "stuck", w)
			continue
		}
	}
}

func (l *Leader) snapshot() []core.Assignment {
	var cluster []core.Assignment
	for _, worker := range l.alloc.Workers() {
		assign := l.alloc.Assigned(worker.Instance.ID)

		cluster = append(cluster, core.Assignment{
			Worker: worker.Instance.Data,
			Grants: slicex.Map(append(assign.Active, assign.Revoked...), toGrant),
		})
	}
	return cluster
}

func (l *Leader) handle(ctx context.Context, req HandleRequest) (*internal_v1.LeaderHandleResponse, error) {
	if req.IsMutation() {
		return l.handleWrite(ctx, req)
	}

	switch {
	case req.Proto.GetTenant() != nil:
		ret, err := l.handleTenantRequest(ctx, req.Proto.GetTenant())
		if err != nil {
			return nil, err
		}
		return NewHandleTenantResponse(ret), nil

	case req.Proto.GetService() != nil:
		ret, err := l.handleServiceRequest(ctx, req.Proto.GetService())
		if err != nil {
			return nil, err
		}
		return NewHandleServiceResponse(ret), nil

	case req.Proto.GetDomain() != nil:
		ret, err := l.handleDomainRequest(ctx, req.Proto.GetDomain())
		if err != nil {
			return nil, err
		}
		return NewHandleDomainResponse(ret), nil

	case req.Proto.GetPlacement() != nil:
		ret, err := l.handlePlacementRequest(ctx, req.Proto.GetPlacement())
		if err != nil {
			return nil, err
		}
		return NewHandlePlacementResponse(ret), nil

	default:
		return nil, fmt.Errorf("invalid handle request: %v", req)
	}
}

func (l *Leader) handleTenantRequest(ctx context.Context, req *internal_v1.TenantRequest) (*internal_v1.TenantResponse, error) {
	return syncx.Txn1(ctx, l.txn, func() (*internal_v1.TenantResponse, error) {
		switch {
		case req.GetList() != nil:
			return l.handleListTenantsRequest(ctx, req.GetList())
		case req.GetInfo() != nil:
			return l.handleInfoTenantRequest(ctx, req.GetInfo())
		default:
			return nil, fmt.Errorf("invalid tenant request: %v", req)
		}
	})
}

func (l *Leader) handleListTenantsRequest(ctx context.Context, req *public_v1.ListTenantsRequest) (*internal_v1.TenantResponse, error) {
	return &internal_v1.TenantResponse{
		Resp: &internal_v1.TenantResponse_List{
			List: &public_v1.ListTenantsResponse{
				Tenants: slicex.Map(l.cache.Tenants(), model.UnwrapTenantInfo),
			},
		},
	}, nil
}

func (l *Leader) handleInfoTenantRequest(ctx context.Context, req *public_v1.InfoTenantRequest) (*internal_v1.TenantResponse, error) {
	name := model.TenantName(req.GetName())

	info, ok := l.cache.Tenant(name)
	if !ok {
		return nil, fmt.Errorf("tenant %v not found: %w", name, model.ErrNotFound)
	}

	return &internal_v1.TenantResponse{
		Resp: &internal_v1.TenantResponse_Info{
			Info: &public_v1.InfoTenantResponse{
				Tenant: model.UnwrapTenantInfo(info),
			},
		},
	}, nil
}

func (l *Leader) handleServiceRequest(ctx context.Context, req *internal_v1.ServiceRequest) (*internal_v1.ServiceResponse, error) {
	return syncx.Txn1(ctx, l.txn, func() (*internal_v1.ServiceResponse, error) {
		switch {
		case req.GetList() != nil:
			return l.handleListServicesRequest(ctx, req.GetList())
		case req.GetInfo() != nil:
			return l.handleInfoServiceRequest(ctx, req.GetInfo())
		default:
			return nil, fmt.Errorf("invalid service request: %v", req)
		}
	})
}

func (l *Leader) handleListServicesRequest(ctx context.Context, req *public_v1.ListServicesRequest) (*internal_v1.ServiceResponse, error) {
	return &internal_v1.ServiceResponse{
		Resp: &internal_v1.ServiceResponse_List{
			List: &public_v1.ListServicesResponse{
				Services: slicex.Map(l.cache.Services(model.TenantName(req.GetTenant())), model.UnwrapServiceInfoEx),
			},
		},
	}, nil
}

func (l *Leader) handleInfoServiceRequest(ctx context.Context, req *public_v1.InfoServiceRequest) (*internal_v1.ServiceResponse, error) {
	name, err := model.ParseQualifiedServiceName(req.GetName())
	if err != nil {
		return nil, fmt.Errorf("invalid SQN: %w", model.ErrInvalid)
	}

	info, ok := l.cache.Service(name)
	if !ok {
		return nil, fmt.Errorf("service %v not found: %w", name, model.ErrNotFound)
	}

	return &internal_v1.ServiceResponse{
		Resp: &internal_v1.ServiceResponse_Info{
			Info: &public_v1.InfoServiceResponse{
				Service: model.UnwrapServiceInfoEx(info),
			},
		},
	}, nil
}

func (l *Leader) handleDomainRequest(ctx context.Context, req *internal_v1.DomainRequest) (*internal_v1.DomainResponse, error) {
	return syncx.Txn1(ctx, l.txn, func() (*internal_v1.DomainResponse, error) {
		switch {
		case req.GetList() != nil:
			return l.handleListDomainsRequest(ctx, req.GetList())
		default:
			return nil, fmt.Errorf("invalid domain request: %v", req)
		}
	})
}

func (l *Leader) handleListDomainsRequest(ctx context.Context, req *public_v1.ListDomainsRequest) (*internal_v1.DomainResponse, error) {
	name, err := model.ParseQualifiedServiceName(req.GetService())
	if err != nil {
		return nil, fmt.Errorf("invalid SQN: %w", model.ErrInvalid)
	}

	if _, ok := l.cache.Service(name); !ok {
		return nil, fmt.Errorf("service %v not found: %w", name, model.ErrNotFound)
	}

	return &internal_v1.DomainResponse{
		Resp: &internal_v1.DomainResponse_List{
			List: &public_v1.ListDomainsResponse{
				Domains: slicex.Map(l.cache.Domains(name), model.UnwrapDomain),
			},
		},
	}, nil
}

func (l *Leader) handlePlacementRequest(ctx context.Context, req *internal_v1.PlacementRequest) (*internal_v1.PlacementResponse, error) {
	return syncx.Txn1(ctx, l.txn, func() (*internal_v1.PlacementResponse, error) {
		switch {
		case req.GetList() != nil:
			return l.handleListPlacementsRequest(ctx, req.GetList())
		case req.GetInfo() != nil:
			return l.handleInfoPlacementRequest(ctx, req.GetInfo())
		default:
			return nil, fmt.Errorf("invalid placement request: %v", req)
		}
	})
}

func (l *Leader) handleListPlacementsRequest(ctx context.Context, req *internal_v1.ListPlacementsRequest) (*internal_v1.PlacementResponse, error) {
	name := model.TenantName(req.GetTenant())

	if _, ok := l.cache.Tenant(name); !ok {
		return nil, fmt.Errorf("tenant not found: %w", model.ErrNotFound)
	}

	return &internal_v1.PlacementResponse{
		Resp: &internal_v1.PlacementResponse_List{
			List: &internal_v1.ListPlacementsResponse{
				Info: slicex.Map(l.cache.Placements(name), core.UnwrapInternalPlacementInfo),
			},
		},
	}, nil
}

func (l *Leader) handleInfoPlacementRequest(ctx context.Context, req *internal_v1.InfoPlacementRequest) (*internal_v1.PlacementResponse, error) {
	name, err := model.ParseQualifiedPlacementName(req.GetName())
	if err != nil {
		return nil, fmt.Errorf("invalid PQN: %w", model.ErrInvalid)
	}

	info, ok := l.cache.Placement(name)
	if !ok {
		return nil, fmt.Errorf("placement %v not found: %w", name, model.ErrNotFound)
	}

	return &internal_v1.PlacementResponse{
		Resp: &internal_v1.PlacementResponse_Info{
			Info: &internal_v1.InfoPlacementResponse{
				Info: core.UnwrapInternalPlacementInfo(info),
			},
		},
	}, nil
}

func (l *Leader) handleWrite(ctx context.Context, req HandleRequest) (*internal_v1.LeaderHandleResponse, error) {
	// (1) Storage operation. Validate and enqueue it sync if mutation.

	done, resp, err := syncx.Txn2(ctx, l.txn, func() (iox.AsyncCloser, *internal_v1.LeaderHandleResponse, error) {
		return l.writer.HandleAsync(ctx, req)
	})
	if err != nil {
		return nil, err
	}

	// (2) Wait for commit before returning result.

	select {
	case <-done.Closed():
		return resp, nil
	case <-ctx.Done():
		return nil, model.ErrOverloaded
	case <-l.Closed():
		return nil, model.ErrNotOwned
	}
}

func (l *Leader) emitMetrics(ctx context.Context) {
	l.resetMetrics(ctx)

	numWorkers.Set(ctx, float64(len(l.workers)), core.StatusTag("ok"))
	for _, info := range l.cache.Tenants() {
		count := len(l.cache.Services(info.Name()))
		numServices.Set(ctx, float64(count), core.TenantTag(info.Name()))
	}
}

func (l *Leader) resetMetrics(ctx context.Context) {
	numWorkers.Reset(ctx)
	numServices.Reset(ctx)
}

func (l *Leader) txn(ctx context.Context, fn func() error) error {
	var wg sync.WaitGroup
	var err error

	wg.Add(1)
	select {
	case l.inject <- func() {
		defer wg.Done()
		err = fn()
	}:
		wg.Wait()
		return err
	case <-ctx.Done():
		return model.ErrOverloaded
	case <-l.Closed():
		return model.ErrDraining
	}
}

func recordAction(ctx context.Context, action, result string) {
	numActions.Increment(ctx, 1, core.ActionTag(action), core.ResultTag(result))
}

func recordActions(ctx context.Context, n int, action, result string) {
	numActions.Increment(ctx, n, core.ActionTag(action), core.ResultTag(result))
}
