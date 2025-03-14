package storage

import (
	"fmt"

	"go.atoms.co/lib/mapx"
	"go.atoms.co/splitter/pkg/core"
	"go.atoms.co/splitter/pkg/model"
)

type tenantInfo struct {
	info       model.TenantInfo
	services   map[model.QualifiedServiceName]model.ServiceInfoEx
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

func (c *Cache) Services(name model.TenantName) []model.ServiceInfoEx {
	if t, ok := c.tenants[name]; ok {
		return mapx.MapValues(t.services, func(s model.ServiceInfoEx) model.ServiceInfoEx {
			return s
		})
	}
	return nil
}

func (c *Cache) Service(name model.QualifiedServiceName) (model.ServiceInfoEx, bool) {
	if t, ok := c.tenants[name.Tenant]; ok {
		s, ok := t.services[name]
		return s, ok
	}
	return model.ServiceInfoEx{}, false
}

func (c *Cache) Domains(name model.QualifiedServiceName) []model.Domain {
	if t, ok := c.tenants[name.Tenant]; ok {
		if s, ok := t.services[name]; ok {
			return s.Domains()
		}
	}
	return nil
}

func (c *Cache) Domain(name model.QualifiedDomainName) (model.Domain, bool) {
	if t, ok := c.tenants[name.Service.Tenant]; ok {
		if s, ok := t.services[name.Service]; ok {
			return s.Domain(name.Domain)
		}
	}
	return model.Domain{}, false
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

func (c *Cache) ServiceState(name model.QualifiedServiceName) (core.State, bool) {
	if t, ok := c.tenants[name.Tenant]; ok {
		if s, ok := t.services[name]; ok {
			return core.NewState(t.info, []model.ServiceInfoEx{s}, mapx.Values(t.placements)), ok
		}
	}
	return core.State{}, false
}

func (c *Cache) State(name model.TenantName) (core.State, bool) {
	if t, ok := c.tenants[name]; ok {
		return core.NewState(t.info, mapx.Values(t.services), mapx.Values(t.placements)), ok
	}
	return core.State{}, false
}

func (c *Cache) Snapshot() core.Snapshot {
	return core.NewSnapshot(mapx.MapValues(c.tenants, func(v *tenantInfo) core.State {
		return core.NewState(v.info, mapx.Values(v.services), mapx.Values(v.placements))
	})...)
}

func (c *Cache) Restore(snapshot core.Snapshot) {
	c.tenants = map[model.TenantName]*tenantInfo{}
	for _, s := range snapshot.Tenants() {
		info := s.Tenant()
		c.tenants[info.Name()] = &tenantInfo{
			info:       info,
			services:   mapx.New(s.Services(), model.ServiceInfoEx.Name),
			placements: mapx.New(s.Placements(), core.InternalPlacementInfo.Name),
		}
	}
}

func (c *Cache) Update(update core.Update, strict bool) error {
	s, err := c.cloneTenant(update, strict)
	if err != nil {
		return err
	}

	for _, upd := range update.ServicesUpdated() {
		current, _ := s.services[upd.Service().Name()]
		if err := checkUpdate(update, upd.Service().Name().Tenant, current.Info().Version(), upd.Service().Version(), strict); err != nil {
			return fmt.Errorf("bad service %v info: %v", upd.Service().Name(), err)
		}
		domains := mapx.New(current.Domains(), model.Domain.Name)
		for _, next := range upd.DomainsUpdated() {
			domains[next.Name()] = next
		}
		for _, remove := range upd.DomainsRemoved() {
			delete(domains, remove)
		}
		s.services[upd.Service().Name()] = model.NewServiceInfoEx(upd.Service(), mapx.Values(domains))
	}
	for _, rm := range update.ServicesRemoved() {
		if _, ok := s.services[rm]; !ok {
			return fmt.Errorf("service %v not found", rm)
		}
		delete(s.services, rm)
	}

	for _, upd := range update.PlacementsUpdated() {
		current, _ := s.placements[upd.Name()]
		if err := checkUpdate(update, upd.Name().Tenant, current.Version(), upd.Version(), strict); err != nil {
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
			services:   map[model.QualifiedServiceName]model.ServiceInfoEx{},
			placements: map[model.QualifiedPlacementName]core.InternalPlacementInfo{},
		}, nil
	}

	cp := &tenantInfo{
		info:       s.info,
		services:   mapx.Clone(s.services),
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
