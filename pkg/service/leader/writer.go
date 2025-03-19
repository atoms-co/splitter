package leader

import (
	"context"
	"fmt"

	"atoms.co/lib-go/pkg/clock"
	"go.atoms.co/lib/log"
	"go.atoms.co/lib/iox"
	"go.atoms.co/lib/workqueue"
	"go.atoms.co/splitter/pkg/core"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pkg/storage"
	splitterprivatepb "go.atoms.co/splitter/pb/private"
	splitterpb "go.atoms.co/splitter/pb"
)

const (
	// pendingApplyCapacity is a limit for how many pending updates we allow (which is lost in a crash).
	// If exceeded, we reject the update with ErrOverloaded.
	pendingApplyCapacity = 20
)

// Writer validates and serializes updates to storage. Not thread-safe.
type Writer struct {
	cl clock.Clock

	db     storage.Storage
	writer *storage.Writer
	cache  *storage.Cache       // pre-commit cache
	pool   *workqueue.WorkQueue // single-threaded async apply

	upd chan<- core.Update
	del chan<- core.Delete
	res chan<- core.Restore

	quit iox.AsyncCloser
}

func NewWriter(cl clock.Clock, db storage.Storage) (*Writer, <-chan core.Update, <-chan core.Delete, <-chan core.Restore) {
	upd := make(chan core.Update)
	del := make(chan core.Delete)
	res := make(chan core.Restore)

	ret := &Writer{
		cl:    cl,
		db:    db,
		cache: storage.NewCache(),
		pool:  workqueue.New(1, pendingApplyCapacity),
		upd:   upd,
		del:   del,
		res:   res,
		quit:  iox.NewAsyncCloser(),
	}

	return ret, upd, del, res
}

func (w *Writer) Init(ctx context.Context) (core.Snapshot, error) {
	snapshot, err := w.db.Read(ctx)
	if err != nil {
		return core.Snapshot{}, err
	}
	w.writer = storage.NewWriter(w.cl, snapshot)
	w.cache.Restore(snapshot)

	return snapshot, nil
}

// HandleAsync attempts to update the state as requested. If the request is accepted, it is applied to the writer
// and (tentative) result available. The caller should delay returning the result until it has been successfully
// applied. A write failure is a total leader failure.
func (w *Writer) HandleAsync(ctx context.Context, req HandleRequest) (iox.AsyncCloser, *splitterprivatepb.LeaderHandleResponse, error) {
	if len(w.pool.Chan()) == pendingApplyCapacity {
		log.Warnf(ctx, "Internal: pending applies reached internal limit: %v", pendingApplyCapacity)
		return nil, nil, model.ErrOverloaded
	}
	return w.handle(ctx, req)
}

// UpdatePlacementAsync attempts to update a placement initiated by the leader. Does not wait for response.
func (w *Writer) UpdatePlacementAsync(ctx context.Context, placement core.InternalPlacement, guard model.Version) error {
	if len(w.pool.Chan()) == pendingApplyCapacity {
		log.Warnf(ctx, "Internal: pending applies reached internal limit: %v", pendingApplyCapacity)
		return model.ErrOverloaded
	}
	upd, _, err := w.writer.Placements.Update(placement, guard)
	if err != nil {
		return err
	}
	_ = w.updateAsync(ctx, upd)
	return nil
}

func (w *Writer) handle(ctx context.Context, req HandleRequest) (iox.AsyncCloser, *splitterprivatepb.LeaderHandleResponse, error) {
	switch {
	case req.Proto.GetTenant() != nil:
		done, ret, err := w.handleTenantRequest(ctx, req.Proto.GetTenant())
		if err != nil {
			return nil, nil, err
		}
		return done, NewHandleTenantResponse(ret), nil

	case req.Proto.GetService() != nil:
		done, ret, err := w.handleServiceRequest(ctx, req.Proto.GetService())
		if err != nil {
			return nil, nil, err
		}
		return done, NewHandleServiceResponse(ret), nil

	case req.Proto.GetDomain() != nil:
		done, ret, err := w.handleDomainRequest(ctx, req.Proto.GetDomain())
		if err != nil {
			return nil, nil, err
		}
		return done, NewHandleDomainResponse(ret), nil

	case req.Proto.GetPlacement() != nil:
		done, ret, err := w.handlePlacementRequest(ctx, req.Proto.GetPlacement())
		if err != nil {
			return nil, nil, err
		}
		return done, NewHandlePlacementResponse(ret), nil

	case req.Proto.GetOperation() != nil:
		done, ret, err := w.handleOperationRequest(ctx, req.Proto.GetOperation())
		if err != nil {
			return nil, nil, err
		}
		return done, NewHandleOperationResponse(ret), nil

	default:
		return nil, nil, fmt.Errorf("invalid handle request: %w", model.ErrInvalid)
	}
}

func (w *Writer) handleTenantRequest(ctx context.Context, req *splitterprivatepb.TenantRequest) (iox.AsyncCloser, *splitterprivatepb.TenantResponse, error) {
	switch {
	case req.GetNew() != nil:
		return w.handleNewTenantRequest(ctx, req.GetNew())
	case req.GetUpdate() != nil:
		return w.handleUpdateTenantRequest(ctx, req.GetUpdate())
	case req.GetDelete() != nil:
		return w.handleDeleteTenantRequest(ctx, req.GetDelete())
	default:
		return nil, nil, fmt.Errorf("invalid mutation")
	}
}

func (w *Writer) handleNewTenantRequest(ctx context.Context, req *splitterpb.NewTenantRequest) (iox.AsyncCloser, *splitterprivatepb.TenantResponse, error) {
	name := model.TenantName(req.GetName())

	var opts []model.TenantOption
	if cfg := req.GetConfig(); cfg != nil {
		opts = append(opts, model.WithTenantConfig(model.WrapTenantConfig(cfg)))
	}
	tenant, err := model.NewTenant(name, w.cl.Now(), opts...)
	if err != nil {
		return nil, nil, err
	}

	upd, info, err := w.writer.Tenants.Create(tenant)
	if err != nil {
		return nil, nil, err
	}
	done := w.updateAsync(ctx, upd)

	return done, &splitterprivatepb.TenantResponse{
		Resp: &splitterprivatepb.TenantResponse_New{
			New: &splitterpb.NewTenantResponse{
				Tenant: model.UnwrapTenantInfo(info),
			},
		},
	}, nil
}

func (w *Writer) handleUpdateTenantRequest(ctx context.Context, req *splitterpb.UpdateTenantRequest) (iox.AsyncCloser, *splitterprivatepb.TenantResponse, error) {
	name := model.TenantName(req.GetName())
	guard := model.Version(req.GetVersion())

	info, ok := w.cache.Tenant(name)
	if !ok {
		return nil, nil, fmt.Errorf("tenant %v not found: %w", name, model.ErrNotFound)
	}

	var opts []model.TenantOption
	if req.GetConfig() != nil {
		opts = append(opts, model.WithTenantConfig(model.WrapTenantConfig(req.GetConfig())))
	}
	if req.GetOperational() != nil {
		opts = append(opts, model.WithTenantOperational(model.WrapTenantOperational(req.GetOperational())))
	}
	if len(opts) == 0 {
		return nil, nil, fmt.Errorf("no tenant options: %w", model.ErrInvalid)
	}
	tenant, err := model.UpdateTenant(info.Tenant(), opts...)
	if err != nil {
		return nil, nil, err
	}

	upd, info, err := w.writer.Tenants.Update(tenant, guard)
	if err != nil {
		return nil, nil, err
	}
	done := w.updateAsync(ctx, upd)

	return done, &splitterprivatepb.TenantResponse{
		Resp: &splitterprivatepb.TenantResponse_Update{
			Update: &splitterpb.UpdateTenantResponse{
				Tenant: model.UnwrapTenantInfo(info),
			},
		},
	}, nil
}

func (w *Writer) handleDeleteTenantRequest(ctx context.Context, req *splitterpb.DeleteTenantRequest) (iox.AsyncCloser, *splitterprivatepb.TenantResponse, error) {
	name := model.TenantName(req.GetName())

	del, err := w.writer.Tenants.Delete(name)
	if err != nil {
		return nil, nil, err
	}
	done := w.deleteAsync(ctx, del)

	return done, &splitterprivatepb.TenantResponse{
		Resp: &splitterprivatepb.TenantResponse_Delete{
			Delete: &splitterpb.DeleteTenantResponse{},
		},
	}, nil
}

func (w *Writer) handleServiceRequest(ctx context.Context, req *splitterprivatepb.ServiceRequest) (iox.AsyncCloser, *splitterprivatepb.ServiceResponse, error) {
	switch {
	case req.GetNew() != nil:
		return w.handleNewServiceRequest(ctx, req.GetNew())
	case req.GetUpdate() != nil:
		return w.handleUpdateServiceRequest(ctx, req.GetUpdate())
	case req.GetDelete() != nil:
		return w.handleDeleteServiceRequest(ctx, req.GetDelete())
	default:
		return nil, nil, fmt.Errorf("invalid mutation")
	}
}

func (w *Writer) handleNewServiceRequest(ctx context.Context, req *splitterpb.NewServiceRequest) (iox.AsyncCloser, *splitterprivatepb.ServiceResponse, error) {
	name, err := model.ParseQualifiedServiceName(req.GetName())
	if err != nil {
		return nil, nil, fmt.Errorf("invalid SQN: %w", model.ErrInvalid)
	}

	var opts []model.ServiceOption
	if cfg := req.GetConfig(); cfg != nil {
		opts = append(opts, model.WithServiceConfig(model.WrapServiceConfig(cfg)))
	}
	service, err := model.NewService(name, w.cl.Now(), opts...)
	if err != nil {
		return nil, nil, err
	}

	upd, info, err := w.writer.Services.Create(service)
	if err != nil {
		return nil, nil, err
	}
	done := w.updateAsync(ctx, upd)

	return done, &splitterprivatepb.ServiceResponse{
		Resp: &splitterprivatepb.ServiceResponse_New{
			New: &splitterpb.NewServiceResponse{
				Service: model.UnwrapServiceInfo(info),
			},
		},
	}, nil
}

func (w *Writer) handleUpdateServiceRequest(ctx context.Context, req *splitterpb.UpdateServiceRequest) (iox.AsyncCloser, *splitterprivatepb.ServiceResponse, error) {
	name, err := model.ParseQualifiedServiceName(req.GetName())
	if err != nil {
		return nil, nil, fmt.Errorf("invalid SQN: %w", model.ErrInvalid)
	}
	guard := model.Version(req.GetVersion())

	infoEx, ok := w.cache.Service(name)
	if !ok {
		return nil, nil, fmt.Errorf("service %v not found: %w", name, model.ErrNotFound)
	}

	var opts []model.ServiceOption
	if req.GetConfig() != nil {
		opts = append(opts, model.WithServiceConfig(model.WrapServiceConfig(req.GetConfig())))
	}
	if req.GetOperational() != nil {
		opts = append(opts, model.WithServiceOperational(model.WrapServiceOperational(req.GetOperational())))
	}
	if len(opts) == 0 {
		return nil, nil, fmt.Errorf("no service options: %w", model.ErrInvalid)
	}
	service, err := model.UpdateService(infoEx.Info().Service(), opts...)
	if err != nil {
		return nil, nil, err
	}

	upd, info, err := w.writer.Services.Update(service, guard)
	if err != nil {
		return nil, nil, err
	}
	done := w.updateAsync(ctx, upd)

	return done, &splitterprivatepb.ServiceResponse{
		Resp: &splitterprivatepb.ServiceResponse_Update{
			Update: &splitterpb.UpdateServiceResponse{
				Service: model.UnwrapServiceInfo(info),
			},
		},
	}, nil
}

func (w *Writer) handleDeleteServiceRequest(ctx context.Context, req *splitterpb.DeleteServiceRequest) (iox.AsyncCloser, *splitterprivatepb.ServiceResponse, error) {
	name, err := model.ParseQualifiedServiceName(req.GetName())
	if err != nil {
		return nil, nil, fmt.Errorf("invalid SQN: %w", model.ErrInvalid)
	}

	upd, err := w.writer.Services.Delete(name)
	if err != nil {
		return nil, nil, err
	}
	done := w.updateAsync(ctx, upd)

	return done, &splitterprivatepb.ServiceResponse{
		Resp: &splitterprivatepb.ServiceResponse_Delete{
			Delete: &splitterpb.DeleteServiceResponse{},
		},
	}, nil
}

func (w *Writer) handleDomainRequest(ctx context.Context, req *splitterprivatepb.DomainRequest) (iox.AsyncCloser, *splitterprivatepb.DomainResponse, error) {
	switch {
	case req.GetNew() != nil:
		return w.handleNewDomainRequest(ctx, req.GetNew())
	case req.GetUpdate() != nil:
		return w.handleUpdateDomainRequest(ctx, req.GetUpdate())
	case req.GetDelete() != nil:
		return w.handleDeleteDomainRequest(ctx, req.GetDelete())
	default:
		return nil, nil, fmt.Errorf("invalid mutation")
	}
}

func (w *Writer) handleNewDomainRequest(ctx context.Context, req *splitterpb.NewDomainRequest) (iox.AsyncCloser, *splitterprivatepb.DomainResponse, error) {
	name, err := model.ParseQualifiedDomainName(req.GetName())
	if err != nil {
		return nil, nil, fmt.Errorf("invalid DQN: %w", model.ErrInvalid)
	}

	var opts []model.DomainOption
	if req.GetConfig() != nil {
		opts = append(opts, model.WithDomainConfig(model.WrapDomainConfig(req.GetConfig())))
	}
	if req.GetState() != 0 {
		opts = append(opts, model.WithDomainState(req.GetState()))
	}
	domain, err := model.NewDomain(name, req.GetType(), w.cl.Now(), opts...)
	if err != nil {
		return nil, nil, err
	}

	upd, info, err := w.writer.Domains.Create(domain)
	if err != nil {
		return nil, nil, err
	}
	done := w.updateAsync(ctx, upd)

	return done, &splitterprivatepb.DomainResponse{
		Resp: &splitterprivatepb.DomainResponse_New{
			New: &splitterpb.NewDomainResponse{
				Domain: model.UnwrapDomain(info),
			},
		},
	}, nil
}

func (w *Writer) handleUpdateDomainRequest(ctx context.Context, req *splitterpb.UpdateDomainRequest) (iox.AsyncCloser, *splitterprivatepb.DomainResponse, error) {
	name, err := model.ParseQualifiedDomainName(req.GetName())
	if err != nil {
		return nil, nil, fmt.Errorf("invalid DQN: %w", model.ErrInvalid)
	}

	service, ok := w.cache.Service(name.Service)
	if !ok {
		return nil, nil, fmt.Errorf("service %v not found: %w", name, model.ErrNotFound)
	}
	guard := service.Info().Version()

	existing, ok := service.Domain(name.Domain)
	if !ok {
		return nil, nil, fmt.Errorf("domain %v not found: %w", name.Domain, model.ErrNotFound)
	}

	var opts []model.DomainOption
	if req.GetConfig() != nil {
		opts = append(opts, model.WithDomainConfig(model.WrapDomainConfig(req.GetConfig())))
	}
	if req.GetState() != 0 {
		opts = append(opts, model.WithDomainState(req.GetState()))
	}
	if req.GetOperational() != nil {
		opts = append(opts, model.WithDomainOperational(model.WrapDomainOperational(req.GetOperational())))
	}
	cur, err := model.UpdateDomain(existing, opts...)
	if err != nil {
		return nil, nil, err
	}

	upd, domain, err := w.writer.Domains.Update(cur, guard)
	if err != nil {
		return nil, nil, err
	}
	done := w.updateAsync(ctx, upd)

	return done, &splitterprivatepb.DomainResponse{
		Resp: &splitterprivatepb.DomainResponse_Update{
			Update: &splitterpb.UpdateDomainResponse{
				Domain: model.UnwrapDomain(domain),
			},
		},
	}, nil
}

func (w *Writer) handleDeleteDomainRequest(ctx context.Context, req *splitterpb.DeleteDomainRequest) (iox.AsyncCloser, *splitterprivatepb.DomainResponse, error) {
	name, err := model.ParseQualifiedDomainName(req.GetName())
	if err != nil {
		return nil, nil, fmt.Errorf("invalid DQN: %w", model.ErrInvalid)
	}

	upd, err := w.writer.Domains.Delete(name)
	if err != nil {
		return nil, nil, err
	}
	done := w.updateAsync(ctx, upd)

	return done, &splitterprivatepb.DomainResponse{
		Resp: &splitterprivatepb.DomainResponse_Delete{
			Delete: &splitterpb.DeleteDomainResponse{},
		},
	}, nil
}

func (w *Writer) handlePlacementRequest(ctx context.Context, req *splitterprivatepb.PlacementRequest) (iox.AsyncCloser, *splitterprivatepb.PlacementResponse, error) {
	switch {
	case req.GetNew() != nil:
		return w.handleNewPlacementRequest(ctx, req.GetNew())
	case req.GetUpdate() != nil:
		return w.handleUpdatePlacementRequest(ctx, req.GetUpdate())
	case req.GetDelete() != nil:
		return w.handleDeletePlacementRequest(ctx, req.GetDelete())
	default:
		return nil, nil, fmt.Errorf("invalid mutation")
	}
}

func (w *Writer) handleNewPlacementRequest(ctx context.Context, req *splitterprivatepb.NewPlacementRequest) (iox.AsyncCloser, *splitterprivatepb.PlacementResponse, error) {
	name, err := model.ParseQualifiedPlacementName(req.GetName())
	if err != nil {
		return nil, nil, fmt.Errorf("invalid PQN: %w", model.ErrInvalid)
	}
	cfg, err := core.ParseInternalPlacementConfig(req.GetConfig())
	if err != nil {
		return nil, nil, err
	}
	placement := core.NewInternalPlacement(name, cfg, w.cl.Now())

	upd, info, err := w.writer.Placements.Create(placement)
	if err != nil {
		return nil, nil, err
	}
	done := w.updateAsync(ctx, upd)

	return done, &splitterprivatepb.PlacementResponse{
		Resp: &splitterprivatepb.PlacementResponse_New{
			New: &splitterprivatepb.NewPlacementResponse{
				Info: core.UnwrapInternalPlacementInfo(info),
			},
		},
	}, nil
}

func (w *Writer) handleUpdatePlacementRequest(ctx context.Context, req *splitterprivatepb.UpdatePlacementRequest) (iox.AsyncCloser, *splitterprivatepb.PlacementResponse, error) {
	name, err := model.ParseQualifiedPlacementName(req.GetName())
	if err != nil {
		return nil, nil, fmt.Errorf("invalid PQN: %w", model.ErrInvalid)
	}
	guard := model.Version(req.GetVersion())

	cur, ok := w.cache.Placement(name)
	if !ok {
		return nil, nil, model.ErrNotFound
	}
	if cur.InternalPlacement().IsDecommissioned() {
		return nil, nil, fmt.Errorf("decommissioned: %w", model.ErrNotAllowed)
	}

	var opts []core.UpdateInternalPlacementOption

	if req.GetState() != 0 {
		opts = append(opts, core.WithInternalPlacementState(req.GetState()))
	}
	if req.GetConfig() != nil {
		cfg, err := core.ParseInternalPlacementConfig(req.GetConfig())
		if err != nil {
			return nil, nil, fmt.Errorf("invalid config: %w", err)
		}
		opts = append(opts, core.WithInternalPlacementConfig(cfg))
	}

	if len(opts) == 0 {
		return nil, nil, fmt.Errorf("empty update: %w", model.ErrInvalid)
	}
	placement := core.UpdateInternalPlacement(cur.InternalPlacement(), opts...)

	upd, info, err := w.writer.Placements.Update(placement, guard)
	if err != nil {
		return nil, nil, err
	}
	done := w.updateAsync(ctx, upd)

	return done, &splitterprivatepb.PlacementResponse{
		Resp: &splitterprivatepb.PlacementResponse_Update{
			Update: &splitterprivatepb.UpdatePlacementResponse{
				Info: core.UnwrapInternalPlacementInfo(info),
			},
		},
	}, nil
}

func (w *Writer) handleDeletePlacementRequest(ctx context.Context, req *splitterprivatepb.DeletePlacementRequest) (iox.AsyncCloser, *splitterprivatepb.PlacementResponse, error) {
	name, err := model.ParseQualifiedPlacementName(req.GetName())
	if err != nil {
		return nil, nil, fmt.Errorf("invalid PQN: %w", model.ErrInvalid)
	}

	// Validate that it is decommissioned and not used. If we don't enforce decommissioned state, an uncommitted
	// apply ahead of us might assign it, and then we would delete into an invalid state.

	cur, ok := w.cache.Placement(name)
	if !ok {
		return nil, nil, model.ErrNotFound
	}
	if !cur.InternalPlacement().IsDecommissioned() {
		return nil, nil, fmt.Errorf("not decommissioned: %w", model.ErrNotAllowed)
	}
	for _, s := range w.cache.Services(name.Tenant) {
		for _, d := range w.cache.Domains(s.Name()) {
			if p, ok := d.Config().Placement(); ok && p == name.Placement {
				return nil, nil, fmt.Errorf("stil in use: %w", model.ErrNotAllowed)
			}
		}
	}

	upd, err := w.writer.Placements.Delete(name)
	if err != nil {
		return nil, nil, err
	}
	done := w.updateAsync(ctx, upd)

	return done, &splitterprivatepb.PlacementResponse{
		Resp: &splitterprivatepb.PlacementResponse_Delete{
			Delete: &splitterprivatepb.DeletePlacementResponse{},
		},
	}, nil
}

func (w *Writer) handleOperationRequest(ctx context.Context, req *splitterprivatepb.OperationRequest) (iox.AsyncCloser, *splitterprivatepb.OperationResponse, error) {
	switch {
	case req.GetRestore() != nil:
		return w.handleNewRestoreRequest(ctx, req.GetRestore())
	default:
		return nil, nil, fmt.Errorf("invalid mutation")
	}
}

func (w *Writer) handleNewRestoreRequest(ctx context.Context, req *splitterprivatepb.RestoreRequest) (iox.AsyncCloser, *splitterprivatepb.OperationResponse, error) {
	nuke := req.GetNuke()
	snapshot := req.GetSnapshot()

	var res core.Restore
	if nuke {
		res = core.NewRestore(core.NewSnapshot())
	} else if snapshot != nil {
		res = core.NewRestore(core.WrapSnapshot(snapshot))
	} else {
		res = w.writer.Restore()
	}
	done := w.restoreAsync(ctx, res)

	return done, &splitterprivatepb.OperationResponse{
		Resp: &splitterprivatepb.OperationResponse_Restore{
			Restore: &splitterprivatepb.RestoreResponse{
				Snapshot: core.UnwrapSnapshot(res.Snapshot()),
			},
		},
	}, nil
}

// TODO(hurwitz) 9/13/2023: We may want to treat raft.Apply failures more like transient DB failures.
// In that case, retry, then returning a failure to the caller may be more appropriate.

func (w *Writer) updateAsync(ctx context.Context, upd core.Update) iox.AsyncCloser {
	if err := w.cache.Update(upd, true); err != nil {
		log.Fatalf(ctx, "Internal: pre-commit cache inconsistent with writer: %v", err)
	}

	done := iox.NewAsyncCloser()
	w.pool.Chan() <- func() {
		defer done.Close()

		// Perform I/O async. If it fails, escalate.

		if err := w.db.Update(ctx, upd); err != nil {
			log.Errorf(ctx, "Failed to apply update: %v. Closing", err)
			w.Close()
			return
		}

		select {
		case w.upd <- upd:
		case <-w.Closed():
		}

	}
	return done
}

func (w *Writer) deleteAsync(ctx context.Context, del core.Delete) iox.AsyncCloser {
	if err := w.cache.Delete(del); err != nil {
		log.Fatalf(ctx, "Internal: pre-commit cache inconsistent with writer: %v", err)
	}

	done := iox.NewAsyncCloser()
	w.pool.Chan() <- func() {
		defer done.Close()

		// Perform I/O async. If it fails, escalate.

		if err := w.db.Delete(ctx, del); err != nil {
			log.Errorf(ctx, "Failed to apply delete: %v. Closing", err)
			w.Close()
			return
		}

		select {
		case w.del <- del:
		case <-w.Closed():
		}
	}
	return done
}

func (w *Writer) restoreAsync(ctx context.Context, res core.Restore) iox.AsyncCloser {
	w.cache.Restore(res.Snapshot())

	done := iox.NewAsyncCloser()
	w.pool.Chan() <- func() {
		defer done.Close()

		// Perform I/O async. If it fails, escalate.

		if err := w.db.Restore(ctx, res); err != nil {
			log.Errorf(ctx, "Failed to apply restore: %v. Closing", err)
			w.Close()
			return
		}

		select {
		case w.res <- res:
		case <-w.Closed():
		}
	}
	return done
}

func (w *Writer) Close() {
	w.pool.Close()
	w.quit.Close()
}

func (w *Writer) Closed() <-chan struct{} {
	return w.quit.Closed()
}
