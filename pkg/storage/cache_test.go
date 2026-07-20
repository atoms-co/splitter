package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"go.atoms.co/splitter/pkg/core"
	"go.atoms.co/splitter/pkg/model"
)

const (
	tenantName  model.TenantName  = "tenantName"
	serviceName model.ServiceName = "serviceName"
)

var qsn = model.QualifiedServiceName{Tenant: tenantName, Service: serviceName}

func TestCache_SnapshotAndRestoreStatus(t *testing.T) {
	deps := createDeps(t, false)

	_, ok := deps.cache.ServiceStatus(deps.service.Name())
	require.False(t, ok)

	restored := NewCache()
	restored.Restore(deps.cache.Snapshot())
	_, ok = restored.ServiceStatus(deps.service.Name())
	require.False(t, ok)

	enabledCfg := model.NewServiceConfig(model.WithTrackLoad(true))
	service, err := model.UpdateService(deps.service, model.WithServiceConfig(enabledCfg))
	require.NoError(t, err)
	serviceInfo := model.NewServiceInfo(service, deps.serviceInfo.Version()+1, time.Now())
	require.NoError(t, restored.Update(core.NewServiceUpdate(serviceInfo), false))
	require.NoError(t, restored.Update(core.NewServiceStatusUpdate(serviceStatus(service.Name(), domainLoad(deps.domain.Name()))), false))
	_, ok = restored.ServiceStatus(service.Name())
	require.True(t, ok)

	restored2 := NewCache()
	restored2.Restore(restored.Snapshot())

	tenant, ok := restored2.tenants[tenantName]
	require.True(t, ok)
	require.Contains(t, tenant.statuses, service.Name())

	_, ok = restored2.ServiceStatus(service.Name())
	require.True(t, ok)
}

func TestCache_StatusRemoval(t *testing.T) {
	t.Run("tenant removed", func(t *testing.T) {
		deps := createDeps(t, true)
		require.NoError(t, deps.cache.Update(core.NewServiceStatusUpdate(serviceStatus(deps.service.Name(), domainLoad(deps.domain.Name()))), false))
		_, ok := deps.cache.ServiceStatus(deps.service.Name())
		require.True(t, ok)

		require.NoError(t, deps.cache.Delete(core.NewDelete(tenantName)))
		_, ok = deps.cache.ServiceStatus(deps.service.Name())
		require.False(t, ok)

		snap := deps.cache.Snapshot()
		require.Empty(t, snap.Tenants())
		require.False(t, ok)
	})

	t.Run("service removed", func(t *testing.T) {
		deps := createDeps(t, true)
		require.NoError(t, deps.cache.Update(core.NewServiceStatusUpdate(serviceStatus(deps.service.Name(), domainLoad(deps.domain.Name()))), false))
		_, ok := deps.cache.ServiceStatus(deps.service.Name())
		require.True(t, ok)

		require.NoError(t, deps.cache.Update(core.NewServiceRemoval(deps.service.Name()), false))
		_, ok = deps.cache.ServiceStatus(deps.service.Name())
		require.False(t, ok)

		snap := deps.cache.Snapshot()
		require.Empty(t, snap.Tenants()[0].Statuses())
	})

	t.Run("service track load disabled", func(t *testing.T) {
		deps := createDeps(t, true)
		require.NoError(t, deps.cache.Update(core.NewServiceStatusUpdate(serviceStatus(deps.service.Name(), domainLoad(deps.domain.Name()))), false))
		_, ok := deps.cache.ServiceStatus(deps.service.Name())
		require.True(t, ok)

		cfg := model.NewServiceConfig(model.WithTrackLoad(false))
		service, err := model.UpdateService(deps.service, model.WithServiceConfig(cfg))
		require.NoError(t, err)

		serviceInfo := model.NewServiceInfo(service, deps.serviceInfo.Version()+1, time.Now())
		require.NoError(t, deps.cache.Update(core.NewServiceUpdate(serviceInfo), false))

		_, ok = deps.cache.ServiceStatus(deps.service.Name())
		require.False(t, ok)

		snap := deps.cache.Snapshot()
		require.Empty(t, snap.Tenants()[0].Statuses())
		_, ok = deps.cache.ServiceStatus(deps.service.Name())
		require.False(t, ok)
	})

	t.Run("domain removed", func(t *testing.T) {
		deps := createDeps(t, true)
		require.NoError(t, deps.cache.Update(core.NewServiceStatusUpdate(serviceStatus(deps.service.Name(), domainLoad(deps.domain.Name()))), false))
		_, ok := deps.cache.ServiceStatus(deps.service.Name())
		require.True(t, ok)

		serviceInfo := model.NewServiceInfo(deps.service, deps.serviceInfo.Version()+1, time.Now())
		require.NoError(t, deps.cache.Update(core.NewDomainRemoval(serviceInfo, deps.domain.Name()), false))

		status, ok := deps.cache.ServiceStatus(deps.service.Name())
		require.True(t, ok)
		require.True(t, serviceLoadHasDomain(status.Load(), deps.domain.Name()))

		snap := deps.cache.Snapshot()
		require.NotEmpty(t, snap.Tenants()[0].Statuses())
	})
}

type cacheTestDeps struct {
	cache       *Cache
	tenant      model.Tenant
	service     model.Service
	serviceInfo model.ServiceInfo
	domain      model.Domain
}

func createDeps(t *testing.T, trackLoad bool) cacheTestDeps {
	t.Helper()

	cache := NewCache()

	tenant, err := model.NewTenant(tenantName, time.Now())
	require.NoError(t, err)

	cfg := model.NewServiceConfig(model.WithTrackLoad(trackLoad))
	service, err := model.NewService(qsn, time.Now(), model.WithServiceConfig(cfg))
	require.NoError(t, err)

	qdn := model.QualifiedDomainName{Service: service.Name(), Domain: model.DomainName("domain1")}
	domain, err := model.NewDomain(qdn, model.Unit, time.Now())
	require.NoError(t, err)

	// update tenant, service and domains.
	require.NoError(t, cache.Update(core.NewTenantUpdate(model.NewTenantInfo(tenant, 1, time.Now())), false))
	serviceInfo := model.NewServiceInfo(service, 1, time.Now())
	require.NoError(t, cache.Update(core.NewDomainUpdate(serviceInfo, domain), false))

	return cacheTestDeps{
		cache:       cache,
		tenant:      tenant,
		service:     service,
		serviceInfo: serviceInfo,
		domain:      domain,
	}
}

func serviceStatus(qsn model.QualifiedServiceName, loads ...core.DomainLoadInfo) core.ServiceStatus {
	serviceLoad := core.NewServiceLoadInfo(qsn, loads)
	status := core.NewServiceStatus(serviceLoad)
	return status
}

func domainLoad(domain model.QualifiedDomainName) core.DomainLoadInfo {
	snap := core.NewP2QuantileSnapshot(
		0.5,
		[]float64{10, 10, 10, 10, 10},
		[]uint64{1, 2, 3, 4, 10},
		[]float64{1, 2, 3, 4, 10},
	)
	trackers := core.NewDomainTrackerSnapshot(time.Now(), snap, nil)
	return core.NewDomainLoadInfo(domain.Domain, trackers, nil)
}

func serviceLoadHasDomain(load core.ServiceLoadInfo, domain model.QualifiedDomainName) bool {
	if load.Service() != domain.Service {
		return false
	}
	for _, domainLoad := range load.Domains() {
		if domainLoad.DomainName() == domain.Domain {
			return true
		}
	}
	return false
}
