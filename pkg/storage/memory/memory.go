package memory

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
	_ storage.Domains    = (*domains)(nil)
	_ storage.Placements = (*placements)(nil)
)

type tenantInfo struct {
	info       model.TenantInfo
	domains    map[model.QualifiedDomainName]model.DomainInfo
	placements map[model.QualifiedPlacementName]core.InternalPlacementInfo
}

// Storage is an in-memory management storage and provider. Intended for single-node installations with no need for
// persistence, notably tests. Thread-safe.
type Storage struct {
	cl clock.Clock

	tenants map[model.TenantName]*tenantInfo
	mu      sync.RWMutex
}

func New(cl clock.Clock) *Storage {
	s := &Storage{
		cl:      cl,
		tenants: map[model.TenantName]*tenantInfo{},
	}
	return s
}

func (s *Storage) Tenants() storage.Tenants {
	return (*tenants)(s)
}

func (s *Storage) Domains() storage.Domains {
	return (*domains)(s)
}

func (s *Storage) Placements() storage.Placements {
	return (*placements)(s)
}

func (s *Storage) Restore(tenants []model.TenantInfo, domains []model.DomainInfo, placements []core.InternalPlacementInfo) {
	s.mu.Lock()
	defer s.mu.Unlock()

	upd := make(map[model.TenantName]*tenantInfo)
	for _, tenant := range tenants {
		upd[tenant.Name()] = &tenantInfo{
			info:       tenant,
			domains:    make(map[model.QualifiedDomainName]model.DomainInfo),
			placements: make(map[model.QualifiedPlacementName]core.InternalPlacementInfo),
		}
	}
	for _, domain := range domains {
		upd[domain.Name().Service.Tenant].domains[domain.Name()] = domain
	}
	for _, placement := range placements {
		upd[placement.Name().Tenant].placements[placement.Name()] = placement
	}

	s.tenants = upd
}

type tenants Storage

func (t *tenants) List(ctx context.Context) ([]model.TenantInfo, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

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
		domains:    map[model.QualifiedDomainName]model.DomainInfo{},
		placements: map[model.QualifiedPlacementName]core.InternalPlacementInfo{},
	}
	return nil
}

func (t *tenants) Read(ctx context.Context, name model.TenantName) (model.TenantInfo, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	info, ok := t.tenants[name]
	if !ok {
		return model.TenantInfo{}, model.ErrNotFound
	}

	return info.info, nil
}

func (t *tenants) Update(ctx context.Context, tenant model.Tenant, guard model.Version) (model.TenantInfo, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	info, ok := t.tenants[tenant.Name()]
	if !ok {
		return model.TenantInfo{}, model.ErrNotFound
	}
	if info.info.Version() != guard {
		return model.TenantInfo{}, model.ErrVersionMismatch
	}

	upd := model.NewTenantInfo(tenant, info.info.Version()+1, t.cl.Now())
	t.tenants[tenant.Name()].info = upd
	return upd, nil
}

func (t *tenants) Delete(ctx context.Context, name model.TenantName) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	delete(t.tenants, name)
	return nil
}

type domains Storage

func (d *domains) List(ctx context.Context, tenant model.TenantName) ([]model.DomainInfo, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	info, ok := d.tenants[tenant]
	if !ok {
		return nil, model.ErrNotFound
	}

	return mapx.Values(info.domains), nil
}

func (d *domains) New(ctx context.Context, domain model.Domain) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	name := domain.Name()

	info, ok := d.tenants[name.Service.Tenant]
	if !ok {
		return model.ErrNotFound
	}
	if _, ok := info.domains[name]; ok {
		return model.ErrAlreadyExists
	}

	info.domains[name] = model.NewDomainInfo(domain, 1, d.cl.Now())
	return nil
}

func (d *domains) Read(ctx context.Context, name model.QualifiedDomainName) (model.DomainInfo, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	info, ok := d.tenants[name.Service.Tenant]
	if !ok {
		return model.DomainInfo{}, model.ErrNotFound
	}
	ret, ok := info.domains[name]
	if !ok {
		return model.DomainInfo{}, model.ErrNotFound
	}

	return ret, nil
}

func (d *domains) Update(ctx context.Context, domain model.Domain, guard model.Version) (model.DomainInfo, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	name := domain.Name()

	info, ok := d.tenants[name.Service.Tenant]
	if !ok {
		return model.DomainInfo{}, model.ErrNotFound
	}
	old, ok := info.domains[name]
	if !ok {
		return model.DomainInfo{}, model.ErrNotFound
	}
	if old.Version() != guard {
		return model.DomainInfo{}, model.ErrVersionMismatch
	}

	upd := model.NewDomainInfo(domain, old.Version()+1, d.cl.Now())
	info.domains[name] = upd
	return upd, nil
}

func (d *domains) Delete(ctx context.Context, name model.QualifiedDomainName) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if info, ok := d.tenants[name.Service.Tenant]; ok {
		if domain, ok := info.domains[name]; ok {
			if domain.Domain().State() != model.DomainSuspended {
				return model.ErrNotAllowed
			}

			delete(info.domains, name)
			return nil
		}
	}

	return nil
}

type placements Storage

func (p *placements) List(ctx context.Context, tenant model.TenantName) ([]core.InternalPlacementInfo, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

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
	if _, ok := info.placements[name]; ok {
		return core.InternalPlacementInfo{}, model.ErrAlreadyExists
	}

	ret := core.NewInternalPlacementInfo(placement, 1, p.cl.Now())
	info.placements[name] = ret
	return ret, nil
}

func (p *placements) Read(ctx context.Context, name model.QualifiedPlacementName) (core.InternalPlacementInfo, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	info, ok := p.tenants[name.Tenant]
	if !ok {
		return core.InternalPlacementInfo{}, model.ErrNotFound
	}
	ret, ok := info.placements[name]
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
	old, ok := info.placements[name]
	if !ok {
		return core.InternalPlacementInfo{}, model.ErrNotFound
	}
	if old.Version() != guard {
		return core.InternalPlacementInfo{}, model.ErrVersionMismatch
	}

	ret := core.NewInternalPlacementInfo(placement, old.Version()+1, p.cl.Now())
	info.placements[name] = ret
	return ret, nil
}

func (p *placements) Delete(ctx context.Context, name model.QualifiedPlacementName) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if info, ok := p.tenants[name.Tenant]; ok {
		if placement, ok := info.placements[name]; ok {
			if placement.InternalPlacement().State() != core.PlacementDecommissioned {
				return model.ErrNotAllowed
			}

			delete(info.placements, name)
			return nil
		}
	}

	return nil
}
