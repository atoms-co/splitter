package storage

import (
	"go.atoms.co/lib/mapx"
	"go.atoms.co/splitter/pkg/core"
	"go.atoms.co/splitter/pkg/model"
	"fmt"
)

type tenantInfo struct {
	info       model.TenantInfo
	domains    map[model.QualifiedDomainName]model.DomainInfo
	placements map[model.QualifiedPlacementName]core.InternalPlacementInfo
}

// Cache is an in-memory cache of state. Not thread-safe.
type Cache struct {
	tenants map[model.TenantName]*tenantInfo
}

func NewCache() *Cache {
	return &Cache{tenants: map[model.TenantName]*tenantInfo{}}
}

func (c *Cache) Tenants() []model.TenantInfo {
	return mapx.MapValues(c.tenants, func(v *tenantInfo) model.TenantInfo {
		return v.info
	})
}

func (c *Cache) Tenant(name model.TenantName) (model.TenantInfo, bool) {
	if t, ok := c.tenants[name]; ok {
		return t.info, true
	}
	return model.TenantInfo{}, false
}

func (c *Cache) Domains(name model.TenantName) []model.DomainInfo {
	if t, ok := c.tenants[name]; ok {
		return mapx.Values(t.domains)
	}
	return nil
}

func (c *Cache) Domain(name model.QualifiedDomainName) (model.DomainInfo, bool) {
	if t, ok := c.tenants[name.Service.Tenant]; ok {
		info, ok := t.domains[name]
		return info, ok
	}
	return model.DomainInfo{}, false
}

func (c *Cache) Placements(name model.TenantName) []core.InternalPlacementInfo {
	if t, ok := c.tenants[name]; ok {
		return mapx.Values(t.placements)
	}
	return nil
}

func (c *Cache) Placement(name model.QualifiedPlacementName) (core.InternalPlacementInfo, bool) {
	if t, ok := c.tenants[name.Tenant]; ok {
		info, ok := t.placements[name]
		return info, ok
	}
	return core.InternalPlacementInfo{}, false
}

func (c *Cache) Snapshot() core.Snapshot {
	return core.NewSnapshot(mapx.MapValues(c.tenants, func(v *tenantInfo) core.State {
		return core.NewState(v.info, mapx.Values(v.domains), mapx.Values(v.placements))
	})...)
}

func (c *Cache) Restore(snapshot core.Snapshot) {
	c.tenants = map[model.TenantName]*tenantInfo{}
	for _, s := range snapshot.Tenants() {
		info := s.Tenant()

		c.tenants[info.Name()] = &tenantInfo{
			info:       info,
			domains:    mapx.New(s.Domains(), model.DomainInfo.Name),
			placements: mapx.New(s.Placements(), core.InternalPlacementInfo.Name),
		}
	}
}

func (c *Cache) Update(update core.Update, strict bool) error {
	s, err := c.cloneTenant(update, strict)
	if err != nil {
		return err
	}

	for _, upd := range update.DomainsUpdated() {
		d, _ := s.domains[upd.Name()]
		if err := checkUpdate(update, upd.Name().Service.Tenant, d.Version(), upd.Version(), strict); err != nil {
			return fmt.Errorf("bad domain %v info: %v", upd.Name(), err)
		}
		s.domains[upd.Name()] = upd
	}
	for _, rm := range update.DomainsRemoved() {
		if _, ok := s.domains[rm]; !ok {
			return fmt.Errorf("domain %v not found", rm)
		}
		delete(s.domains, rm)
	}

	for _, upd := range update.PlacementsUpdated() {
		d, _ := s.placements[upd.Name()]
		if err := checkUpdate(update, upd.Name().Tenant, d.Version(), upd.Version(), strict); err != nil {
			return fmt.Errorf("bad placement %v info: %v", upd.Name(), err)
		}
		s.placements[upd.Name()] = upd
	}
	for _, rm := range update.PlacementsRemoved() {
		if _, ok := s.placements[rm]; !ok {
			return fmt.Errorf("placement %v not found", rm)
		}
		delete(s.placements, rm)
	}

	c.tenants[update.Name()] = s
	return nil
}

func (c *Cache) Delete(del core.Delete) error {
	if _, ok := c.tenants[del.Tenant()]; !ok {
		return fmt.Errorf("tenant %v not found", del.Tenant())
	}

	delete(c.tenants, del.Tenant())
	return nil
}

func (c *Cache) cloneTenant(update core.Update, strict bool) (*tenantInfo, error) {
	s, ok := c.tenants[update.Name()]
	if !ok {
		info, ok := update.TenantUpdated()
		if !ok {
			return nil, fmt.Errorf("tenant %v not found", update.Name())
		}
		if err := checkUpdate(update, info.Name(), 0, info.Version(), strict); err != nil {
			return nil, fmt.Errorf("bad tenant info: %v", err)
		}

		return &tenantInfo{
			info:       info,
			domains:    map[model.QualifiedDomainName]model.DomainInfo{},
			placements: map[model.QualifiedPlacementName]core.InternalPlacementInfo{},
		}, nil
	}

	cp := &tenantInfo{
		info:       s.info,
		domains:    mapx.Clone(s.domains),
		placements: mapx.Clone(s.placements),
	}

	if info, ok := update.TenantUpdated(); ok {
		if err := checkUpdate(update, info.Name(), cp.info.Version(), info.Version(), strict); err != nil {
			return nil, fmt.Errorf("bad tenant info: %v", err)
		}
		cp.info = info
	}
	return cp, nil
}

func checkUpdate(update core.Update, name model.TenantName, current, next model.Version, strict bool) error {
	if isMismatchedTenant(update, name) {
		return fmt.Errorf("tenant %v update included incorrect tenant %v", update.Name(), name)
	}
	if isInvalidVersion(current, next, strict) {
		return fmt.Errorf("tenant %v update included invalid update v%v -> v%v", update.Name(), current, next)
	}
	return nil
}

// isInvalidVersion returns true if the version change is not valid.
func isInvalidVersion(current, next model.Version, strict bool) bool {
	if current < next {
		return strict && current+1 != next
	}
	return true
}

// isMismatchedTenant returns true if the tenant is not the given one.
func isMismatchedTenant(update core.Update, name model.TenantName) bool {
	return update.Name() != name
}
