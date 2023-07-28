package memdb

import (
	"context"
	"go.atoms.co/splitter/pkg/core"
	"sync"

	"atoms.co/lib-go/pkg/clock"
	"go.atoms.co/lib/mapx"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pkg/storage"
)

var (
	_ storage.Tenants    = (*tenants)(nil)
	_ storage.Placements = (*placements)(nil)
)

type tenantInfo struct {
	info       model.TenantInfo
	placements map[model.PlacementName]core.InternalPlacementInfo
}

// Storage is an in-memory management storage and provider. Intended for single-node installations with no need for
// persistence, notably tests. Thread-safe.
type Storage struct {
	cl clock.Clock

	tenants map[model.TenantName]*tenantInfo
	mu      sync.Mutex
}

func New(cl clock.Clock) (*Storage, storage.Management) {
	d := &Storage{
		cl:      cl,
		tenants: map[model.TenantName]*tenantInfo{},
	}
	return d, storage.Management{
		Tenants:    (*tenants)(d),
		Placements: (*placements)(d),
	}
}

type tenants Storage

func (t *tenants) List(ctx context.Context) ([]model.TenantInfo, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	return mapx.MapValues(t.tenants, func(v *tenantInfo) model.TenantInfo {
		return v.info
	}), nil
}

func (t *tenants) New(ctx context.Context, tenant model.Tenant) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if _, ok := t.tenants[tenant.Name()]; ok {
		return model.ErrAlreadyExists
	}

	t.tenants[tenant.Name()] = &tenantInfo{
		info:       model.NewTenantInfo(tenant, 1, t.cl.Now()),
		placements: map[model.PlacementName]core.InternalPlacementInfo{},
	}
	return nil
}

func (t *tenants) Read(ctx context.Context, name model.TenantName) (model.TenantInfo, error) {
	info, ok := t.tenants[name]
	if !ok {
		return model.TenantInfo{}, model.ErrNotFound
	}

	return info.info, nil
}

func (t *tenants) Update(ctx context.Context, tenant model.Tenant, guard storage.UpdateGuard) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	info, ok := t.tenants[tenant.Name()]
	if !ok {
		return model.ErrNotFound
	}
	if info.info.Version() != guard.Guard {
		return model.ErrVersionMismatch
	}

	t.tenants[tenant.Name()].info = model.NewTenantInfo(info.info.Tenant(), info.info.Version()+1, t.cl.Now())
	return nil
}

func (t *tenants) Delete(ctx context.Context, name model.TenantName) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	delete(t.tenants, name)
	return nil
}

type placements Storage

func (p *placements) List(ctx context.Context, tenant model.TenantName) ([]core.InternalPlacementInfo, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	info, ok := p.tenants[tenant]
	if !ok {
		return nil, model.ErrNotFound
	}

	return mapx.Values(info.placements), nil
}

func (p *placements) Create(ctx context.Context, placement core.InternalPlacement) (core.InternalPlacementInfo, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	name := placement.Name()

	info, ok := p.tenants[name.Tenant]
	if !ok {
		return core.InternalPlacementInfo{}, model.ErrNotFound
	}
	if _, ok := info.placements[name.Placement]; ok {
		return core.InternalPlacementInfo{}, model.ErrAlreadyExists
	}

	ret := core.NewInternalPlacementInfo(placement, 1, p.cl.Now())
	info.placements[name.Placement] = ret
	return ret, nil
}

func (p *placements) Read(ctx context.Context, name model.QualifiedPlacementName) (core.InternalPlacementInfo, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	info, ok := p.tenants[name.Tenant]
	if !ok {
		return core.InternalPlacementInfo{}, model.ErrNotFound
	}
	ret, ok := info.placements[name.Placement]
	if !ok {
		return core.InternalPlacementInfo{}, model.ErrNotFound
	}

	return ret, nil
}

func (p *placements) Update(ctx context.Context, placement core.InternalPlacement, guard model.Version) (core.InternalPlacementInfo, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	name := placement.Name()

	info, ok := p.tenants[name.Tenant]
	if !ok {
		return core.InternalPlacementInfo{}, model.ErrNotFound
	}
	old, ok := info.placements[name.Placement]
	if !ok {
		return core.InternalPlacementInfo{}, model.ErrNotFound
	}
	if old.Version() != guard {
		return core.InternalPlacementInfo{}, model.ErrVersionMismatch
	}

	ret := core.NewInternalPlacementInfo(placement, old.Version()+1, p.cl.Now())
	info.placements[name.Placement] = ret
	return ret, nil
}

func (p *placements) Delete(ctx context.Context, name model.QualifiedPlacementName) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if info, ok := p.tenants[name.Tenant]; ok {
		if placement, ok := info.placements[name.Placement]; ok {
			if placement.InternalPlacement().State() != core.PlacementDecommissioned {
				return model.ErrNotAllowed
			}

			delete(info.placements, name.Placement)
			return nil
		}
	}

	return nil
}
