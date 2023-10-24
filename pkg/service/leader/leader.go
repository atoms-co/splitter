package leader

import (
	"context"
	"atoms.co/lib-go/pkg/clock"
	"atoms.co/lib-go/pkg/dist/allocation"
	"go.atoms.co/splitter/lib/service/location"
	"go.atoms.co/splitter/lib/service/session"
	"go.atoms.co/lib/log"
	"go.atoms.co/lib/metrics"
	"go.atoms.co/lib/contextx"
	"go.atoms.co/lib/iox"
	"go.atoms.co/lib/mapx"
	"go.atoms.co/lib/randx"
	"go.atoms.co/slicex"
	"go.atoms.co/lib/syncx"
	"go.atoms.co/splitter/pkg/core"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pkg/service/worker"
	"go.atoms.co/splitter/pkg/storage"
	"go.atoms.co/splitter/pkg/util/sessionx"
	"go.atoms.co/splitter/pkg/util/txnx"
	"go.atoms.co/splitter/pb/private"
	"go.atoms.co/splitter/pb"
	"fmt"
	"time"
)

const (
	// handleTimeout is the timeout for handle requests.
	handleTimeout = 10 * time.Second
	// leaseDuration is the granted lease duration for tenants. It should be long enough to accommodate a reconnect,
	// but not so long tenants are idle for too long if the connection has restarted.
	leaseDuration = 1 * time.Minute
)

var (
	numWorkers = metrics.NewTrackedGauge(metrics.NewGauge("go.atoms.co/splitter/leader_workers", "Connected worker status", core.StatusKey))
	numTenants = metrics.NewTrackedGauge(metrics.NewGauge("go.atoms.co/splitter/leader_tenants", "Tenant status", core.TenantKey, core.StatusKey))

	numActions = metrics.NewCounter("go.atoms.co/splitter/leader_actions", "Leader actions", core.ActionKey, core.ResultKey)
)

type workerSession struct {
	instance   model.Instance
	grants     map[model.TenantName]core.Grant
	draining   bool
	lease      time.Time
	connection sessionx.Connection[Message]
}

func (w *workerSession) String() string {
	return fmt.Sprintf("session{instance=%v, connection=%v}", w.instance, w.connection)
}

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
	alloc    *allocation.Allocation[model.TenantName]
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
		id:          location.NewInstance(loc),
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

func (l *Leader) Join(ctx context.Context, sid session.ID, instance model.Instance, grants []Grant, in <-chan Message) (<-chan Message, error) {
	return syncx.Txn1(ctx, txnx.Txn(l, l.inject), func() (<-chan Message, error) {
		existing, ok := l.workers[instance.ID()]
		if ok {
			existing.connection.Close()
		}

		wctx, cancel := contextx.WithQuitCancel(context.Background(), l.Closed())

		now := l.cl.Now()
		s, out := l.connect(wctx, now, sid, instance, grants, in)
		l.allocate(ctx, now, false) // Allocate to assign work post connection

		go func() {
			defer cancel()
			<-s.connection.Closed()

			syncx.AsyncTxn(txnx.Txn(l, l.inject), func() {
				cur, ok := l.workers[instance.ID()]
				if ok && cur.connection.Sid() == sid { // guard against race
					l.disconnect(wctx, cur)
				}
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
		l.recordAction(ctx, "init", "failed")
		return
	}
	l.cache.Restore(snapshot)

	delay := leaseDuration
	if l.fastActivation {
		delay = 0
	}
	l.alloc = allocation.New[model.TenantName](slicex.Map(l.cache.Tenants(), model.TenantInfo.Name), now.Add(delay), func(unit model.TenantName) location.Location {
		info, _ := l.cache.Tenant(unit)
		if r, ok := info.Tenant().Region(); ok {
			return location.Location{
				Region: location.Region(r),
			}
		}
		return location.Location{Region: "global"}
	})

	if l.drain.IsClosed() {
		log.Errorf(ctx, "Unexpected: leader %v lost leadership while loading", l.id)
		l.recordAction(ctx, "init", "aborted")
		return
	}

	log.Infof(ctx, "Leader initialized: %v, #tenants=%v", l.id, len(snapshot.Tenants()))
	l.recordAction(ctx, "init", "ok")
	l.initialized.Close()

	l.process(ctx)

	log.Infof(ctx, "Leader exited: %v", l.id)
}

// TODO(herohde) 9/2/2013: unclear how leader election transitions. Immediately? If we want a quick drain
// to refresh lease updates etc, we may want need a matching startup delay to guarantee exclusivity.

func (l *Leader) process(ctx context.Context) {
	defer l.resetMetrics(ctx)

	ticker := l.cl.NewTicker(4*time.Second + randx.Duration(time.Second))
	defer ticker.Stop()

steady:
	for {
		select {

		case m := <-l.messages:
			w, ok := l.workers[m.Instance.ID()]
			if !ok || w.connection.Sid() != m.Sid {
				log.Debugf(ctx, "Ignoring stale message %v: %v", m.Instance, m.Msg)
				break
			}
			if err := l.handleMessage(ctx, w, m.Msg); err != nil {
				log.Errorf(ctx, "Error handling worker message %v: %v", w, err)
				l.disconnect(ctx, w)
				break
			}

		case <-ticker.C:
			// Regularly send out lease updates to healthy workers, disconnect unhealthy ones.

			now := l.cl.Now()
			lease := now.Add(leaseDuration)

			var unhealthy []*workerSession
			for _, w := range l.workers {
				if !w.connection.Send(ctx, NewWorkerMessage(worker.NewLeaseUpdateMessage(lease))) {
					unhealthy = append(unhealthy, w)
					continue
				}
				_, _ = l.alloc.Extend(allocation.DemandID(w.instance.ID()), lease)
			}
			log.Debugf(ctx, "Sent heartbeat to %v/%v sessions", len(l.workers)-len(unhealthy), len(l.workers))

			l.disconnect(ctx, unhealthy...)
			l.allocate(ctx, now, true)

			l.emitMetrics(ctx)

		case fn := <-l.inject:
			fn()

		case upd := <-l.upd:
			// TODO: 09/14/23 make changes, allocate

			if err := l.cache.Update(upd, false); err != nil {
				log.Errorf(ctx, "Internal: inconsistent tenant state: %v", err)
				return
			}

		case del := <-l.del:
			// TODO: 09/14/23 remove coordinator

			if err := l.cache.Delete(del); err != nil {
				log.Errorf(ctx, "Internal: inconsistent tenant state: %v", err)
				return
			}

		case res := <-l.res:
			// TODO: 09/25/23 make changes, allocate, remove coordinator if necessary
			l.cache.Restore(res.Snapshot())

		case <-l.drain.Closed():
			break steady

		case <-l.writer.Closed():
			return

		case <-l.Closed():
			return
		}
	}

	log.Infof(ctx, "Leader %v draining, #workers=%v", l.id, len(l.workers))

	for _, w := range l.workers {
		w.connection.Send(ctx, NewWorkerMessage(worker.NewDisconnect()))
	}
	l.cl.Sleep(100 * time.Millisecond) // Delay to give time to send Disconnects before closing connections
	for _, w := range l.workers {
		w.connection.Close()
	}
}

func (l *Leader) handleMessage(ctx context.Context, w *workerSession, m Message) error {
	msg, ok := m.WorkerMessage()
	if !ok {
		return fmt.Errorf("invalid message from worker %v", m)
	}
	switch {
	case msg.IsDeregister():
		deregister, _ := msg.Deregister()
		return l.handleDeregister(ctx, w, deregister)
	case msg.IsRelinquished():
		relinquished, _ := msg.Relinquished()
		return l.handleRelinquished(ctx, w, relinquished)
	default:
		return fmt.Errorf("invalid worker message %v", msg)
	}
}

func (l *Leader) handleDeregister(ctx context.Context, w *workerSession, deregister worker.Deregister) error {
	log.Infof(ctx, "Received de-register from worker %v", w)

	w.draining = true
	_, _ = l.alloc.Disconnect(allocation.DemandID(w.instance.ID()))

	if len(w.grants) == 0 {
		l.disconnect(ctx, w) // no grants to revoke, safe to disconnect
		return nil
	}

	// TODO(jhhurwitz): 10/23/23 We don't actually revoke in the allocation, but rather disconnect the Demand and send
	// revokes to the worker. When the worker relinquishes, since it is disconnected we are able to reactivate the
	// grant. This is an odd way to deregister.
	log.Infof(ctx, "revoking all grants from draining worker %v", w)
	if !w.connection.Send(ctx, NewWorkerMessage(worker.NewRevoke(mapx.Values(w.grants)...))) {
		return fmt.Errorf("unable to send revoke to worker %v", w)
	}

	return nil
}

func (l *Leader) handleRelinquished(ctx context.Context, w *workerSession, relinquished worker.Relinquished) error {
	return nil
}

func (l *Leader) connect(ctx context.Context, now time.Time, sid session.ID, instance model.Instance, grants []Grant, in <-chan Message) (*workerSession, <-chan Message) {
	log.Infof(ctx, "Worker %v connected (session=%v) with #grants: %d", instance, sid, len(grants))

	connection, out := sessionx.NewConnection[Message](l.cl, sid, instance, l, in, l.messages)
	s := &workerSession{
		instance:   instance,
		lease:      now.Add(leaseDuration),
		connection: connection,
	}
	l.workers[instance.ID()] = s // overwrite existing session with extended lease

	demand := allocation.Demand{ID: allocation.DemandID(instance.ID()), Location: instance.Location()}

	_ = l.alloc.Connect(demand)
	// TODO(jhhurwitz): 10/19/23 Capture existing assignments + Send cluster updates

	_, _ = l.alloc.Extend(demand.ID, s.lease)
	connection.Send(ctx, NewWorkerMessage(worker.NewLeaseUpdateMessage(s.lease))) // grants will be covered under this lease

	return l.workers[instance.ID()], out
}

func (l *Leader) disconnect(ctx context.Context, workers ...*workerSession) {
	for _, w := range workers {
		log.Infof(ctx, "Disconnecting worker: %v", w)
		tenants, _ := l.alloc.Disconnect(allocation.DemandID(w.instance.ID()))
		log.Infof(ctx, "Unassigned %v tenants from worker %v", len(tenants), w)

		delete(l.workers, w.instance.ID())
		w.connection.Send(ctx, NewWorkerMessage(worker.NewDisconnect()))
		w.connection.Close()
	}
}

func (l *Leader) allocate(ctx context.Context, now time.Time, loadbalance bool) {
	// (1) Free any expired grants to make them available for re-allocation.

	freed := l.alloc.Expire(now)
	if len(freed) > 0 {
		log.Infof(ctx, "Expired %v grant(s): %v", len(freed), freed)
	}

	// (2) Allocate and commit the assignments by sending grants. We remove the clients from the alloc
	// on disconnect, so we know that any beneficiary of Allocate has a valid session.

	assignments := l.alloc.Allocate(now)
	for id, grants := range assignments {
		w := l.workers[model.InstanceID(id)]

		for _, grant := range grants {
			tenant := fromAllocationGrant(grant)
			log.Debugf(ctx, "Allocating new grant to worker %v: %v.", w, tenant)

			state, _ := l.cache.State(tenant.Tenant())
			if !w.connection.Send(ctx, NewWorkerMessage(worker.NewAssign(tenant, state))) {
				log.Warnf(ctx, "Failed to send grant % to worker: %w, disconnecting", tenant, w)
				l.disconnect(ctx, w)
				break
			}
		}
		_, _ = l.alloc.Extend(id, w.lease) // shorten the grant leases to the worker lease
	}

	// TODO(jhhurwitz): 10/23/23 Cluster-map broadcast

	// (4) Load-balance grants across workers, if needed. Free up some grants from the most overloaded workers, but
	// only 1 tenant per allocate call.
	if loadbalance {
		deficiencies := l.alloc.LoadBalance()
		if len(deficiencies) > 0 {
			d := deficiencies[0]
			w := l.workers[model.InstanceID(d.Demand)]
			if w.connection.Send(ctx, NewWorkerMessage(worker.NewRevoke(fromAllocationGrant(d.Grant)))) {
				_, _ = l.alloc.Revoke(d.Demand, d.Grant)
			} else {
				log.Warnf(ctx, "Failed to revoke deficiency % to worker: %w, disconnecting", d, w)
				l.disconnect(ctx, w)
			}
		}
	}
	log.Debugf(ctx, "Allocation: %v", l.alloc)
}

func fromAllocationGrant(grant allocation.Grant[model.TenantName]) core.Grant {
	return core.NewGrant(grant.ID, grant.Unit, grant.Lease, grant.Assigned)
}

func toAllocationGrant(grant core.Grant) allocation.Grant[model.TenantName] {
	return allocation.NewGrant[model.TenantName](grant.ID(), grant.Tenant(), grant.Lease(), grant.Assigned())

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
		return nil, fmt.Errorf("invalid request")
	}
}

func (l *Leader) handleTenantRequest(ctx context.Context, req *internal_v1.TenantRequest) (*internal_v1.TenantResponse, error) {
	return syncx.Txn1(ctx, txnx.Txn(l, l.inject), func() (*internal_v1.TenantResponse, error) {
		switch {
		case req.GetList() != nil:
			return l.handleListTenantsRequest(ctx, req.GetList())
		case req.GetInfo() != nil:
			return l.handleInfoTenantRequest(ctx, req.GetInfo())
		default:
			return nil, fmt.Errorf("invalid request")
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
		return nil, model.ErrNotFound
	}

	return &internal_v1.TenantResponse{
		Resp: &internal_v1.TenantResponse_Info{
			Info: &public_v1.InfoTenantResponse{
				Tenant: model.UnwrapTenantInfo(info),
			},
		},
	}, nil
}

func (l *Leader) handleDomainRequest(ctx context.Context, req *internal_v1.DomainRequest) (*internal_v1.DomainResponse, error) {
	return syncx.Txn1(ctx, txnx.Txn(l, l.inject), func() (*internal_v1.DomainResponse, error) {
		switch {
		case req.GetList() != nil:
			return l.handleListDomainsRequest(ctx, req.GetList())
		case req.GetInfo() != nil:
			return l.handleInfoDomainRequest(ctx, req.GetInfo())
		default:
			return nil, fmt.Errorf("invalid request")
		}
	})
}

func (l *Leader) handleListDomainsRequest(ctx context.Context, req *public_v1.ListDomainsRequest) (*internal_v1.DomainResponse, error) {
	name := model.TenantName(req.GetTenant())

	if _, ok := l.cache.Tenant(name); !ok {
		return nil, model.ErrNotFound
	}

	return &internal_v1.DomainResponse{
		Resp: &internal_v1.DomainResponse_List{
			List: &public_v1.ListDomainsResponse{
				Domains: slicex.Map(l.cache.Domains(name), model.UnwrapDomainInfo),
			},
		},
	}, nil
}

func (l *Leader) handleInfoDomainRequest(ctx context.Context, req *public_v1.InfoDomainRequest) (*internal_v1.DomainResponse, error) {
	name, err := model.ParseQualifiedDomainName(req.GetName())
	if err != nil {
		return nil, model.ErrInvalid
	}

	info, ok := l.cache.Domain(name)
	if !ok {
		return nil, model.ErrNotFound
	}

	return &internal_v1.DomainResponse{
		Resp: &internal_v1.DomainResponse_Info{
			Info: &public_v1.InfoDomainResponse{
				Domain: model.UnwrapDomainInfo(info),
			},
		},
	}, nil
}

func (l *Leader) handlePlacementRequest(ctx context.Context, req *internal_v1.PlacementRequest) (*internal_v1.PlacementResponse, error) {
	return syncx.Txn1(ctx, txnx.Txn(l, l.inject), func() (*internal_v1.PlacementResponse, error) {
		switch {
		case req.GetList() != nil:
			return l.handleListPlacementsRequest(ctx, req.GetList())
		case req.GetInfo() != nil:
			return l.handleInfoPlacementRequest(ctx, req.GetInfo())
		default:
			return nil, fmt.Errorf("invalid request")
		}
	})
}

func (l *Leader) handleListPlacementsRequest(ctx context.Context, req *internal_v1.ListPlacementsRequest) (*internal_v1.PlacementResponse, error) {
	name := model.TenantName(req.GetTenant())

	if _, ok := l.cache.Tenant(name); !ok {
		return nil, model.ErrNotFound
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
		return nil, model.ErrInvalid
	}

	info, ok := l.cache.Placement(name)
	if !ok {
		return nil, model.ErrNotFound
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

	done, resp, err := syncx.Txn2(ctx, txnx.Txn(l, l.inject), func() (iox.AsyncCloser, *internal_v1.LeaderHandleResponse, error) {
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
		numTenants.Set(ctx, float64(1), core.TenantTag(info.Name()), core.StatusTag("ok"))
	}
}

func (l *Leader) resetMetrics(ctx context.Context) {
	numWorkers.Reset(ctx)
	numTenants.Reset(ctx)
}

func (l *Leader) recordAction(ctx context.Context, action, result string) {
	numActions.Increment(ctx, 1, core.ActionTag(action), core.ResultTag(result))
}
