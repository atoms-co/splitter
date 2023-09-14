package storage

import (
	"atoms.co/lib-go/pkg/clock"
	"go.atoms.co/splitter/pkg/core"
	"go.atoms.co/splitter/pkg/model"
)

type Tenants interface {
	Create(tenant model.Tenant) (Update, model.TenantInfo, error)
	Update(tenant model.Tenant, guard model.Version) (Update, model.TenantInfo, error)
	Delete(name model.TenantName) (Delete, error)
}

type Domains interface {
	Create(domain model.Domain) (Update, model.DomainInfo, error)
	Update(domain model.Domain, guard model.Version) (Update, model.DomainInfo, error)
	Delete(name model.QualifiedDomainName) (Update, error)
}

type Placements interface {
	Create(placement core.InternalPlacement) (Update, core.InternalPlacementInfo, error)
	Update(placement core.InternalPlacement, guard model.Version) (Update, core.InternalPlacementInfo, error)
	Delete(name model.QualifiedPlacementName) (Update, error)
}

// Join merges two updates into one, if possible. It is not possible if tenants are different or
// if an entity is revived, i.e., deleted and re-created. The first update must then be committed.
// Any stacked update must also be the immediate next version.
func Join(current, next Update) (Update, bool) {
	// TODO(herohde) 9/12/2013: implement non-trivial join
	return current, false
}

// Writer validates and sequences updates in-memory, which must then be applied to storage.
// Updates can be Joined to make batch updates to a tenant. The sequencer can be ahead of
// persisted data. Not thread-safe.
type Writer struct {
	Tenants    Tenants
	Domains    Domains
	Placements Placements

	cl clock.Clock
	db *Cache
}

func NewWriter(cl clock.Clock, snapshot Snapshot) *Writer {
	ret := &Writer{
		cl: cl,
		db: NewCache(),
	}
	ret.Tenants = (*tenants)(ret)
	ret.Domains = (*domains)(ret)
	ret.Placements = (*placements)(ret)

	ret.db.Restore(snapshot)
	return ret
}

type tenants Writer

func (t *tenants) Create(tenant model.Tenant) (Update, model.TenantInfo, error) {
	if _, ok := t.db.Tenant(tenant.Name()); ok {
		return Update{}, model.TenantInfo{}, model.ErrAlreadyExists
	}
	return t.update(tenant, 1)
}

func (t *tenants) Update(tenant model.Tenant, guard model.Version) (Update, model.TenantInfo, error) {
	cur, ok := t.db.Tenant(tenant.Name())
	if !ok {
		return Update{}, model.TenantInfo{}, model.ErrNotFound
	}
	if cur.Version() != guard {
		return Update{}, model.TenantInfo{}, model.ErrVersionMismatch
	}
	return t.update(tenant, guard+1)
}

func (t *tenants) Delete(name model.TenantName) (Delete, error) {
	if _, ok := t.db.Tenant(name); !ok {
		return Delete{}, model.ErrNotFound
	}
	del := NewDelete(name)
	return del, t.db.Delete(del)
}

func (t *tenants) update(tenant model.Tenant, version model.Version) (Update, model.TenantInfo, error) {
	info := model.NewTenantInfo(tenant, version, t.cl.Now())
	upd := NewTenantUpdate(info)
	return upd, info, t.db.Update(upd, true)
}

type domains Writer

func (d *domains) Create(domain model.Domain) (Update, model.DomainInfo, error) {
	if _, ok := d.db.Tenant(domain.Name().Service.Tenant); !ok {
		return Update{}, model.DomainInfo{}, model.ErrNotFound
	}
	if _, ok := d.db.Domain(domain.Name()); ok {
		return Update{}, model.DomainInfo{}, model.ErrAlreadyExists
	}
	return d.update(domain, 1)
}

func (d *domains) Update(domain model.Domain, guard model.Version) (Update, model.DomainInfo, error) {
	cur, ok := d.db.Domain(domain.Name())
	if !ok {
		return Update{}, model.DomainInfo{}, model.ErrNotFound
	}
	if cur.Version() != guard {
		return Update{}, model.DomainInfo{}, model.ErrVersionMismatch
	}
	return d.update(domain, guard+1)
}

func (d *domains) Delete(name model.QualifiedDomainName) (Update, error) {
	if _, ok := d.db.Domain(name); !ok {
		return Update{}, model.ErrNotFound
	}
	upd := NewDomainRemoval(name)
	return upd, d.db.Update(upd, true)
}

func (d *domains) update(domain model.Domain, version model.Version) (Update, model.DomainInfo, error) {
	info := model.NewDomainInfo(domain, version, d.cl.Now())
	upd := NewDomainUpdate(info)
	return upd, info, d.db.Update(upd, true)
}

type placements Writer

func (p *placements) Create(placement core.InternalPlacement) (Update, core.InternalPlacementInfo, error) {
	if _, ok := p.db.Tenant(placement.Name().Tenant); !ok {
		return Update{}, core.InternalPlacementInfo{}, model.ErrNotFound
	}
	if _, ok := p.db.Placement(placement.Name()); ok {
		return Update{}, core.InternalPlacementInfo{}, model.ErrAlreadyExists
	}
	return p.update(placement, 1)
}

func (p *placements) Update(placement core.InternalPlacement, guard model.Version) (Update, core.InternalPlacementInfo, error) {
	cur, ok := p.db.Placement(placement.Name())
	if !ok {
		return Update{}, core.InternalPlacementInfo{}, model.ErrNotFound
	}
	if cur.Version() != guard {
		return Update{}, core.InternalPlacementInfo{}, model.ErrVersionMismatch
	}
	return p.update(placement, guard+1)
}

func (p *placements) Delete(name model.QualifiedPlacementName) (Update, error) {
	if _, ok := p.db.Placement(name); !ok {
		return Update{}, model.ErrNotFound
	}
	upd := NewPlacementRemoval(name)
	return upd, p.db.Update(upd, true)
}

func (p *placements) update(placement core.InternalPlacement, version model.Version) (Update, core.InternalPlacementInfo, error) {
	info := core.NewInternalPlacementInfo(placement, version, p.cl.Now())
	upd := NewPlacementUpdate(info)
	return upd, info, p.db.Update(upd, true)
}
