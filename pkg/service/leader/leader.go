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
	"go.atoms.co/lib/randx"
	"go.atoms.co/slicex"
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

// Leader centralizes tenant and storage coordination. All updates go through the global leader, which is
// dynamically selected and may be present at different nodes at different times.
type Leader struct {
	iox.AsyncCloser

	cl clock.Clock
	id location.Instance

	writer *Writer
	cache  *storage.Cache // post-commit cache

	workers map[location.InstanceID]*worker
	alloc   *allocation.Allocation[model.TenantName]

	upd    <-chan storage.Update
	del    <-chan storage.Delete
	inject chan func()

	initialized, drain iox.AsyncCloser
}

func New(ctx context.Context, cl clock.Clock, loc location.Location, db storage.Storage) *Leader {
	writer, upd, del := NewWriter(cl, db)

	ret := &Leader{
		AsyncCloser: iox.NewAsyncCloser(),
		cl:          cl,
		id:          location.NewInstance(loc),
		writer:      writer,
		cache:       storage.NewCache(),
		workers:     map[location.InstanceID]*worker{},
		upd:         upd,
		del:         del,
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
	defer l.writer.Close()

	log.Infof(ctx, "Leader initializing: %v", l.id)

	snapshot, err := l.writer.Init(ctx)
	if err != nil {
		log.Errorf(ctx, "Failed to load tenant data: %v", err)
		l.recordAction(ctx, "init", "failed")
		return
	}
	l.cache.Restore(snapshot)

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

		case <-ticker.C:
			l.emitMetrics(ctx)

		case fn := <-l.inject:
			fn()

		case upd := <-l.upd:
			// TODO: make changes, allocate

			if err := l.cache.Update(upd, false); err != nil {
				log.Errorf(ctx, "Internal: inconsistent tenant state: %v", err)
				return
			}

		case del := <-l.del:
			// TODO: remove coordinator

			if err := l.cache.Delete(del); err != nil {
				log.Errorf(ctx, "Internal: inconsistent tenant state: %v", err)
				return
			}

		case <-l.drain.Closed():
			break steady

		case <-l.writer.Closed():
			return

		case <-l.Closed():
			return
		}
	}

	log.Infof(ctx, "Leader %v draining, #workers=%v", l.id, len(l.workers))
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
	return syncx.Txn1(ctx, l.txn, func() (*internal_v1.TenantResponse, error) {
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
	return syncx.Txn1(ctx, l.txn, func() (*internal_v1.DomainResponse, error) {
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
	return syncx.Txn1(ctx, l.txn, func() (*internal_v1.PlacementResponse, error) {
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
		numTenants.Set(ctx, float64(1), core.TenantTag(info.Name()), core.StatusTag("ok"))
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
