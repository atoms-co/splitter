package storage

import (
	"fmt"
	"time"

	"go.atoms.co/splitter/pkg/core"
	"go.atoms.co/splitter/pkg/model"
)

type Tenants interface {
	Create(tenant model.Tenant) (core.Update, model.TenantInfo, error)
	Update(tenant model.Tenant, guard model.Version) (core.Update, model.TenantInfo, error)
	Delete(name model.TenantName) (core.Delete, error)
}

type Services interface {
	Create(service model.Service) (core.Update, model.ServiceInfo, error)
	Update(service model.Service, guard model.Version) (core.Update, model.ServiceInfo, error)
	Delete(name model.QualifiedServiceName) (core.Update, error)
}

type Domains interface {
	Create(domain model.Domain) (core.Update, model.Domain, error)
	Update(domain model.Domain, guard model.Version) (core.Update, model.Domain, error)
	Delete(name model.QualifiedDomainName) (core.Update, error)
}

type Placements interface {
	Create(placement core.InternalPlacement) (core.Update, core.InternalPlacementInfo, error)
	Update(placement core.InternalPlacement, guard model.Version) (core.Update, core.InternalPlacementInfo, error)
	Delete(name model.QualifiedPlacementName) (core.Update, error)
}

// Join merges two updates into one, if possible. It is not possible if tenants are different or
// if an entity is revived, i.e., deleted and re-created. The first update must then be committed.
// Any stacked update must also be the immediate next version.
func Join(current, next core.Update) (core.Update, bool) {
	// TODO(herohde) 9/12/2013: implement non-trivial join
	return current, false
}

// Writer validates and sequences updates in-memory, which must then be applied to storage.
// Updates can be Joined to make batch updates to a tenant. The sequencer can be ahead of
// persisted data. Not thread-safe.
type Writer struct {
	Tenants    Tenants
	Services   Services
	Domains    Domains
	Placements Placements

	db *Cache
}

func NewWriter(snapshot core.Snapshot) *Writer {
	ret := &Writer{
		db: NewCache(),
	}
	ret.Tenants = (*tenants)(ret)
	ret.Services = (*services)(ret)
	ret.Domains = (*domains)(ret)
	ret.Placements = (*placements)(ret)

	ret.db.Restore(snapshot)
	return ret
}

// Restore returns a Restore struct populated with the current writer snapshot
func (w *Writer) Restore() core.Restore {
	return core.NewRestore(w.db.Snapshot())
}

type tenants Writer

func (t *tenants) Create(tenant model.Tenant) (core.Update, model.TenantInfo, error) {
	if _, ok := t.db.Tenant(tenant.Name()); ok {
		return core.Update{}, model.TenantInfo{}, fmt.Errorf("tenant %v exists: %w", tenant.Name(), model.ErrAlreadyExists)
	}
	return t.update(tenant, 1)
}

func (t *tenants) Update(tenant model.Tenant, guard model.Version) (core.Update, model.TenantInfo, error) {
	cur, ok := t.db.Tenant(tenant.Name())
	if !ok {
		return core.Update{}, model.TenantInfo{}, fmt.Errorf("tenant %v not found: %w", tenant.Name(), model.ErrNotFound)
	}
	if cur.Version() != guard {
		return core.Update{}, model.TenantInfo{}, model.ErrVersionMismatch
	}
	return t.update(tenant, guard+1)
}

func (t *tenants) Delete(name model.TenantName) (core.Delete, error) {
	if _, ok := t.db.Tenant(name); !ok {
		return core.Delete{}, fmt.Errorf("tenant %v not found: %w", name, model.ErrNotFound)
	}
	del := core.NewDelete(name)
	return del, t.db.Delete(del)
}

func (t *tenants) update(tenant model.Tenant, version model.Version) (core.Update, model.TenantInfo, error) {
	info := model.NewTenantInfo(tenant, version, time.Now())
	upd := core.NewTenantUpdate(info)
	return upd, info, t.db.Update(upd, true)
}

type services Writer

func (s *services) Create(service model.Service) (core.Update, model.ServiceInfo, error) {
	if _, ok := s.db.Tenant(service.Name().Tenant); !ok {
		return core.Update{}, model.ServiceInfo{}, fmt.Errorf("tenant %v not found: %w", service.Name().Tenant, model.ErrNotFound)
	}
	if _, ok := s.db.Service(service.Name()); ok {
		return core.Update{}, model.ServiceInfo{}, fmt.Errorf("service %v exists: %w", service.Name(), model.ErrAlreadyExists)
	}
	info := model.NewServiceInfo(service, 1, time.Now())
	upd := core.NewServiceUpdate(info)

	return upd, info, s.db.Update(upd, true)
}

func (s *services) Update(service model.Service, guard model.Version) (core.Update, model.ServiceInfo, error) {
	cur, ok := s.db.Service(service.Name())
	if !ok {
		return core.Update{}, model.ServiceInfo{}, fmt.Errorf("service %v not found: %w", service.Name(), model.ErrNotFound)
	}
	if cur.Info().Version() != guard {
		return core.Update{}, model.ServiceInfo{}, model.ErrVersionMismatch
	}
	info := model.NewServiceInfo(service, guard+1, time.Now())
	upd := core.NewServiceUpdate(info)

	return upd, info, s.db.Update(upd, true)
}

func (s *services) Delete(name model.QualifiedServiceName) (core.Update, error) {
	if _, ok := s.db.Service(name); !ok {
		return core.Update{}, fmt.Errorf("service %v not found: %w", name, model.ErrNotFound)
	}
	upd := core.NewServiceRemoval(name)

	return upd, s.db.Update(upd, true)
}

type domains Writer

func (d *domains) Create(domain model.Domain) (core.Update, model.Domain, error) {
	service, ok := d.db.Service(domain.Name().Service)
	if !ok {
		return core.Update{}, model.Domain{}, fmt.Errorf("service %v not found: %w", domain.Name().Service, model.ErrNotFound)
	}
	if _, ok := d.db.Domain(domain.Name()); ok {
		return core.Update{}, model.Domain{}, fmt.Errorf("domain %v exists: %w", domain.Name(), model.ErrAlreadyExists)
	}
	info := model.NewServiceInfo(service.Info().Service(), service.Info().Version()+1, time.Now())
	upd := core.NewDomainUpdate(info, domain)

	return upd, domain, d.db.Update(upd, true)
}

func (d *domains) Update(domain model.Domain, guard model.Version) (core.Update, model.Domain, error) {
	service, ok := d.db.Service(domain.Name().Service)
	if !ok {
		return core.Update{}, model.Domain{}, fmt.Errorf("service %v not found: %w", domain.Name().Service, model.ErrNotFound)
	}
	if _, ok := d.db.Domain(domain.Name()); !ok {
		return core.Update{}, model.Domain{}, fmt.Errorf("domain %v not found: %w", domain.Name(), model.ErrNotFound)
	}
	if service.Info().Version() != guard {
		return core.Update{}, model.Domain{}, model.ErrVersionMismatch
	}
	info := model.NewServiceInfo(service.Info().Service(), service.Info().Version()+1, time.Now())
	upd := core.NewDomainUpdate(info, domain)

	return upd, domain, d.db.Update(upd, true)
}

func (d *domains) Delete(name model.QualifiedDomainName) (core.Update, error) {
	service, ok := d.db.Service(name.Service)
	if !ok {
		return core.Update{}, fmt.Errorf("service %v not found: %w", name.Service, model.ErrNotFound)
	}
	if _, ok := d.db.Domain(name); !ok {
		return core.Update{}, fmt.Errorf("domain %v not found: %w", name, model.ErrNotFound)
	}
	info := model.NewServiceInfo(service.Info().Service(), service.Info().Version()+1, time.Now())
	upd := core.NewDomainRemoval(info, name)

	return upd, d.db.Update(upd, true)
}

type placements Writer

func (p *placements) Create(placement core.InternalPlacement) (core.Update, core.InternalPlacementInfo, error) {
	if _, ok := p.db.Tenant(placement.Name().Tenant); !ok {
		return core.Update{}, core.InternalPlacementInfo{}, fmt.Errorf("tenant %v not found: %w", placement.Name().Tenant, model.ErrNotFound)
	}
	if _, ok := p.db.Placement(placement.Name()); ok {
		return core.Update{}, core.InternalPlacementInfo{}, fmt.Errorf("placement %v exists: %w", placement.Name(), model.ErrAlreadyExists)
	}
	return p.update(placement, 1)
}

func (p *placements) Update(placement core.InternalPlacement, guard model.Version) (core.Update, core.InternalPlacementInfo, error) {
	cur, ok := p.db.Placement(placement.Name())
	if !ok {
		return core.Update{}, core.InternalPlacementInfo{}, fmt.Errorf("placement %v not found: %w", placement.Name(), model.ErrNotFound)
	}
	if cur.Version() != guard {
		return core.Update{}, core.InternalPlacementInfo{}, model.ErrVersionMismatch
	}
	return p.update(placement, guard+1)
}

func (p *placements) Delete(name model.QualifiedPlacementName) (core.Update, error) {
	if _, ok := p.db.Placement(name); !ok {
		return core.Update{}, fmt.Errorf("placement %v not found: %w", name, model.ErrNotFound)
	}
	upd := core.NewPlacementRemoval(name)
	return upd, p.db.Update(upd, true)
}

func (p *placements) update(placement core.InternalPlacement, version model.Version) (core.Update, core.InternalPlacementInfo, error) {
	info := core.NewInternalPlacementInfo(placement, version, time.Now())
	upd := core.NewPlacementUpdate(info)
	return upd, info, p.db.Update(upd, true)
}
