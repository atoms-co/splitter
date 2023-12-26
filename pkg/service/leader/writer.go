package leader

import (
	"context"
	"atoms.co/lib-go/pkg/clock"
	"go.atoms.co/lib/log"
	"go.atoms.co/lib/iox"
	"go.atoms.co/lib/workqueue"
	"go.atoms.co/splitter/pkg/core"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pkg/storage"
	"go.atoms.co/splitter/pb/private"
	"go.atoms.co/splitter/pb"
	"fmt"
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
func (w *Writer) HandleAsync(ctx context.Context, req HandleRequest) (iox.AsyncCloser, *internal_v1.LeaderHandleResponse, error) {
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

func (w *Writer) handle(ctx context.Context, req HandleRequest) (iox.AsyncCloser, *internal_v1.LeaderHandleResponse, error) {
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

func (w *Writer) handleTenantRequest(ctx context.Context, req *internal_v1.TenantRequest) (iox.AsyncCloser, *internal_v1.TenantResponse, error) {
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

func (w *Writer) handleNewTenantRequest(ctx context.Context, req *public_v1.NewTenantRequest) (iox.AsyncCloser, *internal_v1.TenantResponse, error) {
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

	return done, &internal_v1.TenantResponse{
		Resp: &internal_v1.TenantResponse_New{
			New: &public_v1.NewTenantResponse{
				Tenant: model.UnwrapTenantInfo(info),
			},
		},
	}, nil
}

func (w *Writer) handleUpdateTenantRequest(ctx context.Context, req *public_v1.UpdateTenantRequest) (iox.AsyncCloser, *internal_v1.TenantResponse, error) {
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

	return done, &internal_v1.TenantResponse{
		Resp: &internal_v1.TenantResponse_Update{
			Update: &public_v1.UpdateTenantResponse{
				Tenant: model.UnwrapTenantInfo(info),
			},
		},
	}, nil
}

func (w *Writer) handleDeleteTenantRequest(ctx context.Context, req *public_v1.DeleteTenantRequest) (iox.AsyncCloser, *internal_v1.TenantResponse, error) {
	name := model.TenantName(req.GetName())

	del, err := w.writer.Tenants.Delete(name)
	if err != nil {
		return nil, nil, err
	}
	done := w.deleteAsync(ctx, del)

	return done, &internal_v1.TenantResponse{
		Resp: &internal_v1.TenantResponse_Delete{
			Delete: &public_v1.DeleteTenantResponse{},
		},
	}, nil
}

func (w *Writer) handleServiceRequest(ctx context.Context, req *internal_v1.ServiceRequest) (iox.AsyncCloser, *internal_v1.ServiceResponse, error) {
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

func (w *Writer) handleNewServiceRequest(ctx context.Context, req *public_v1.NewServiceRequest) (iox.AsyncCloser, *internal_v1.ServiceResponse, error) {
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

	return done, &internal_v1.ServiceResponse{
		Resp: &internal_v1.ServiceResponse_New{
			New: &public_v1.NewServiceResponse{
				Service: model.UnwrapServiceInfo(info),
			},
		},
	}, nil
}

func (w *Writer) handleUpdateServiceRequest(ctx context.Context, req *public_v1.UpdateServiceRequest) (iox.AsyncCloser, *internal_v1.ServiceResponse, error) {
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

	return done, &internal_v1.ServiceResponse{
		Resp: &internal_v1.ServiceResponse_Update{
			Update: &public_v1.UpdateServiceResponse{
				Service: model.UnwrapServiceInfo(info),
			},
		},
	}, nil
}

func (w *Writer) handleDeleteServiceRequest(ctx context.Context, req *public_v1.DeleteServiceRequest) (iox.AsyncCloser, *internal_v1.ServiceResponse, error) {
	name, err := model.ParseQualifiedServiceName(req.GetName())
	if err != nil {
		return nil, nil, fmt.Errorf("invalid SQN: %w", model.ErrInvalid)
	}

	upd, err := w.writer.Services.Delete(name)
	if err != nil {
		return nil, nil, err
	}
	done := w.updateAsync(ctx, upd)

	return done, &internal_v1.ServiceResponse{
		Resp: &internal_v1.ServiceResponse_Delete{
			Delete: &public_v1.DeleteServiceResponse{},
		},
	}, nil
}

func (w *Writer) handleDomainRequest(ctx context.Context, req *internal_v1.DomainRequest) (iox.AsyncCloser, *internal_v1.DomainResponse, error) {
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

func (w *Writer) handleNewDomainRequest(ctx context.Context, req *public_v1.NewDomainRequest) (iox.AsyncCloser, *internal_v1.DomainResponse, error) {
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

	return done, &internal_v1.DomainResponse{
		Resp: &internal_v1.DomainResponse_New{
			New: &public_v1.NewDomainResponse{
				Domain: model.UnwrapDomain(info),
			},
		},
	}, nil
}

func (w *Writer) handleUpdateDomainRequest(ctx context.Context, req *public_v1.UpdateDomainRequest) (iox.AsyncCloser, *internal_v1.DomainResponse, error) {
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
	cur, err := model.UpdateDomain(existing, opts...)
	if err != nil {
		return nil, nil, err
	}

	upd, domain, err := w.writer.Domains.Update(cur, guard)
	if err != nil {
		return nil, nil, err
	}
	done := w.updateAsync(ctx, upd)

	return done, &internal_v1.DomainResponse{
		Resp: &internal_v1.DomainResponse_Update{
			Update: &public_v1.UpdateDomainResponse{
				Domain: model.UnwrapDomain(domain),
			},
		},
	}, nil
}

func (w *Writer) handleDeleteDomainRequest(ctx context.Context, req *public_v1.DeleteDomainRequest) (iox.AsyncCloser, *internal_v1.DomainResponse, error) {
	name, err := model.ParseQualifiedDomainName(req.GetName())
	if err != nil {
		return nil, nil, fmt.Errorf("invalid DQN: %w", model.ErrInvalid)
	}

	upd, err := w.writer.Domains.Delete(name)
	if err != nil {
		return nil, nil, err
	}
	done := w.updateAsync(ctx, upd)

	return done, &internal_v1.DomainResponse{
		Resp: &internal_v1.DomainResponse_Delete{
			Delete: &public_v1.DeleteDomainResponse{},
		},
	}, nil
}

func (w *Writer) handlePlacementRequest(ctx context.Context, req *internal_v1.PlacementRequest) (iox.AsyncCloser, *internal_v1.PlacementResponse, error) {
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

func (w *Writer) handleNewPlacementRequest(ctx context.Context, req *internal_v1.NewPlacementRequest) (iox.AsyncCloser, *internal_v1.PlacementResponse, error) {
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

	return done, &internal_v1.PlacementResponse{
		Resp: &internal_v1.PlacementResponse_New{
			New: &internal_v1.NewPlacementResponse{
				Info: core.UnwrapInternalPlacementInfo(info),
			},
		},
	}, nil
}

func (w *Writer) handleUpdatePlacementRequest(ctx context.Context, req *internal_v1.UpdatePlacementRequest) (iox.AsyncCloser, *internal_v1.PlacementResponse, error) {
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

	return done, &internal_v1.PlacementResponse{
		Resp: &internal_v1.PlacementResponse_Update{
			Update: &internal_v1.UpdatePlacementResponse{
				Info: core.UnwrapInternalPlacementInfo(info),
			},
		},
	}, nil
}

func (w *Writer) handleDeletePlacementRequest(ctx context.Context, req *internal_v1.DeletePlacementRequest) (iox.AsyncCloser, *internal_v1.PlacementResponse, error) {
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

	return done, &internal_v1.PlacementResponse{
		Resp: &internal_v1.PlacementResponse_Delete{
			Delete: &internal_v1.DeletePlacementResponse{},
		},
	}, nil
}

func (w *Writer) handleOperationRequest(ctx context.Context, req *internal_v1.OperationRequest) (iox.AsyncCloser, *internal_v1.OperationResponse, error) {
	switch {
	case req.GetRestore() != nil:
		return w.handleNewRestoreRequest(ctx, req.GetRestore())
	default:
		return nil, nil, fmt.Errorf("invalid mutation")
	}
}

func (w *Writer) handleNewRestoreRequest(ctx context.Context, req *internal_v1.RestoreRequest) (iox.AsyncCloser, *internal_v1.OperationResponse, error) {
	nuke := req.GetNuke()

	res := w.writer.Restore(nuke)
	done := w.restoreAsync(ctx, res)

	return done, &internal_v1.OperationResponse{
		Resp: &internal_v1.OperationResponse_Restore{
			Restore: &internal_v1.RestoreResponse{
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
