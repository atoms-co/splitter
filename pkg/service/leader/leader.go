package leader

import (
	"context"
	"atoms.co/lib-go/pkg/clock"
	"atoms.co/lib-go/pkg/dist/allocation"
	"go.atoms.co/splitter/lib/service/location"
	"go.atoms.co/splitter/lib/service/session"
	"go.atoms.co/lib/log"
	"go.atoms.co/lib/metrics"
	"go.atoms.co/lib/iox"
	"go.atoms.co/lib/mapx"
	"go.atoms.co/lib/randx"
	"go.atoms.co/lib/syncx"
	"go.atoms.co/splitter/pkg/core"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pkg/storage"
	"go.atoms.co/splitter/pb/private"
	"go.atoms.co/splitter/pb"
	"fmt"
	"sync"
	"time"
)

const (
	// handleTimeout is the timeout for handle requests.
	handleTimeout = 10 * time.Second
)

var (
	numWorkers = metrics.NewTrackedGauge(metrics.NewGauge("go.atoms.co/splitter/leader_workers", "Connected worker status", core.StatusKey))
	numTenants = metrics.NewTrackedGauge(metrics.NewGauge("go.atoms.co/splitter/leader_tenants", "Tenant status", core.TenantKey, core.StatusKey))

	numActions = metrics.NewCounter("go.atoms.co/splitter/leader_actions", "Leader actions", core.ActionKey, core.ResultKey)
)

type worker struct {
	id  model.Instance
	sid session.ID

	out chan<- JoinMessage
}

type tenant struct {
	info       model.TenantInfo
	domains    map[model.QualifiedDomainName]model.DomainInfo
	placements map[model.PlacementName]core.InternalPlacementInfo
}

// Leader centralizes tenant and storage coordination. All updates go through the global leader, which is
// dynamically selected and may be present at different nodes at different times.
type Leader struct {
	iox.AsyncCloser

	cl clock.Clock
	id location.Instance
	db storage.Storage

	workers map[location.InstanceID]*worker
	tenants map[model.TenantName]*tenant
	alloc   *allocation.Allocation[model.TenantName]

	inject chan func()

	initialized, drain iox.AsyncCloser
}

func New(ctx context.Context, cl clock.Clock, loc location.Location, db storage.Storage) *Leader {
	ret := &Leader{
		AsyncCloser: iox.NewAsyncCloser(),
		cl:          cl,
		id:          location.NewInstance(loc),
		db:          db,
		workers:     map[location.InstanceID]*worker{},
		tenants:     map[model.TenantName]*tenant{},
		inject:      make(chan func()),
		initialized: iox.NewAsyncCloser(),
		drain:       iox.WithQuit(ctx.Done(), iox.NewAsyncCloser()), // context cancel => drain
	}
	go ret.init(context.Background())

	return ret
}

func (l *Leader) Join(ctx context.Context, sid session.ID, id model.Instance, grants []Grant, in <-chan JoinMessage) (<-chan JoinMessage, error) {
	return syncx.Txn1(ctx, l.txn, func() (<-chan JoinMessage, error) {

		return nil, nil
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

func (l *Leader) init(ctx context.Context) {
	defer l.Close()
	defer l.drain.Close()
	defer l.initialized.Close()

	log.Infof(ctx, "Leader initializing: %v", l.id)

	// With the raft implementation, the storage cache is in memory and does not fail -- by design. We
	// do not have good options in other setups if the leader fails to load state.

	tenants, err := l.db.Tenants().List(ctx)
	if err != nil {
		log.Errorf(ctx, "Failed to load tenants: %v", err)
		l.recordAction(ctx, "init", "failed/tenants")
		return
	}
	for _, info := range tenants {
		name := info.Name()

		placements, err := l.db.Placements().List(ctx, name)
		if err != nil {
			log.Errorf(ctx, "Failed to load %v placements: %v", name, err)
			l.recordAction(ctx, "init", "failed/placements")
			return
		}

		domains, err := l.db.Domains().List(ctx, name)
		if err != nil {
			log.Errorf(ctx, "Failed to load %v domains: %v", name, err)
			l.recordAction(ctx, "init", "failed/domains")
			return
		}

		l.tenants[name] = &tenant{
			info:       info,
			domains:    mapx.New(domains, model.DomainInfo.Name),
			placements: mapx.New(placements, core.InternalPlacementInfo.Name),
		}

		log.Infof(ctx, "Loaded tenant %v, #domains=%v, #placements=%v", name, len(domains), len(placements))
	}

	if l.drain.IsClosed() {
		log.Errorf(ctx, "Unexpected: leader %v lost leadership while loading", l.id)
		l.recordAction(ctx, "init", "aborted")
		return
	}

	log.Infof(ctx, "Leader initialized: %v, #tenants=%v", l.id, len(l.tenants))
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

		case <-ticker.C:
			l.emitMetrics(ctx)

		case fn := <-l.inject:
			fn()

		case <-l.drain.Closed():
			break steady

		case <-l.Closed():
			return
		}
	}

	log.Infof(ctx, "Leader %v draining, #workers=%v", l.id, len(l.workers))
}

func (l *Leader) handle(ctx context.Context, req HandleRequest) (*internal_v1.LeaderHandleResponse, error) {
	switch {
	case req.Proto.GetTenant() != nil:
		ret, err := l.handleTenantRequest(ctx, req.Proto.GetTenant())
		if err != nil {
			return nil, err
		}
		return &internal_v1.LeaderHandleResponse{Resp: &internal_v1.LeaderHandleResponse_Tenant{Tenant: ret}}, nil

	case req.Proto.GetPlacement() != nil:
		ret, err := l.handlePlacementRequest(ctx, req.Proto.GetPlacement())
		if err != nil {
			return nil, err
		}
		return &internal_v1.LeaderHandleResponse{Resp: &internal_v1.LeaderHandleResponse_Placement{Placement: ret}}, nil

	default:
		return nil, fmt.Errorf("invalid request")
	}
}

func (l *Leader) handleTenantRequest(ctx context.Context, req *internal_v1.TenantRequest) (*internal_v1.TenantResponse, error) {
	switch {
	case req.GetList() != nil:
		return l.handleListTenantsRequest(ctx)

	default:
		return nil, fmt.Errorf("invalid request")
	}
}

func (l *Leader) handleListTenantsRequest(ctx context.Context) (*internal_v1.TenantResponse, error) {
	return syncx.Txn1(ctx, l.txn, func() (*internal_v1.TenantResponse, error) {
		return &internal_v1.TenantResponse{
			Resp: &internal_v1.TenantResponse_List{
				List: &public_v1.ListTenantsResponse{
					Tenants: mapx.MapValues(l.tenants, func(v *tenant) *public_v1.TenantInfo {
						return model.UnwrapTenantInfo(v.info)
					}),
				},
			},
		}, nil
	})
}
func (l *Leader) handlePlacementRequest(ctx context.Context, req *internal_v1.PlacementRequest) (*internal_v1.PlacementResponse, error) {
	switch {
	case req.GetList() != nil:
		return l.handleListPlacementsRequest(ctx, req.GetList())

	default:
		return nil, fmt.Errorf("invalid request")
	}
}

func (l *Leader) handleListPlacementsRequest(ctx context.Context, req *internal_v1.ListPlacementsRequest) (*internal_v1.PlacementResponse, error) {
	name := model.TenantName(req.GetTenant())

	return syncx.Txn1(ctx, l.txn, func() (*internal_v1.PlacementResponse, error) {
		t, ok := l.tenants[name]
		if !ok {
			return nil, model.ErrNotFound
		}

		return &internal_v1.PlacementResponse{
			Resp: &internal_v1.PlacementResponse_List{
				List: &internal_v1.ListPlacementsResponse{
					Info: mapx.MapValues(t.placements, core.UnwrapInternalPlacementInfo),
				},
			},
		}, nil
	})
}

func (l *Leader) emitMetrics(ctx context.Context) {
	l.resetMetrics(ctx)

	numWorkers.Set(ctx, float64(len(l.workers)), core.StatusTag("ok"))
	for name := range l.tenants {
		numTenants.Set(ctx, float64(1), core.TenantTag(name), core.StatusTag("ok"))
	}
}

func (l *Leader) resetMetrics(ctx context.Context) {
	numWorkers.Reset(ctx)
	numTenants.Reset(ctx)
}

// txn runs the given function in the main thread sync. Any signal that triggers a complex action must
// perform I/O or expensive parts outside txn and potentially use multiple txn calls.
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
	case <-l.Closed():
		return model.ErrNotOwned
	case <-ctx.Done():
		return model.ErrOverloaded
	}
}

func (l *Leader) recordAction(ctx context.Context, action, result string) {
	numActions.Increment(ctx, 1, core.ActionTag(action), core.ResultTag(result))
}

func (l *Leader) String() string {
	return l.id.String()
}
