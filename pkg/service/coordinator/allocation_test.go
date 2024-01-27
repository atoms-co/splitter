package coordinator_test

import (
	"go.atoms.co/splitter/lib/service/location"
	"go.atoms.co/splitter/pkg/allocation"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pkg/service/coordinator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestControl(t *testing.T) {
	t1, err := model.NewTenant("tenant1", time.Time{}, model.WithTenantConfig(
		model.NewTenantConfig(model.WithTenantBannedRegions("centralus")),
	))
	require.NoError(t, err)

	s1, err := model.NewService(model.QualifiedServiceName{Tenant: t1.Name(), Service: "service1"}, time.Time{}, model.WithServiceConfig(
		model.NewServiceConfig(model.WithServiceBannedRegions("northcentralus")),
	))
	require.NoError(t, err)

	d1, err := model.NewDomain(model.QualifiedDomainName{Service: s1.Name(), Domain: "domain1"}, model.Regional, time.Time{}, model.WithDomainConfig(
		model.NewDomainConfig(model.WithDomainShardingPolicy(model.NewShardingPolicy(4))),
	))
	require.NoError(t, err)

	d2, err := model.NewDomain(model.QualifiedDomainName{Service: s1.Name(), Domain: "domain2"}, model.Regional, time.Time{}, model.WithDomainConfig(
		model.NewDomainConfig(model.WithDomainShardingPolicy(model.NewShardingPolicy(4)), model.WithDomainBannedRegions("eastus2")),
	))
	require.NoError(t, err)

	d3, err := model.NewDomain(model.QualifiedDomainName{Service: s1.Name(), Domain: "domain3"}, model.Regional, time.Time{},
		model.WithDomainState(model.DomainSuspended),
		model.WithDomainConfig(
			model.NewDomainConfig(model.WithDomainShardingPolicy(model.NewShardingPolicy(4))),
		))
	require.NoError(t, err)

	tInfo := model.NewTenantInfo(t1, 1, time.Time{})
	s1Info := model.NewServiceInfoEx(model.NewServiceInfo(s1, 1, time.Time{}), []model.Domain{d1, d2, d3})

	ctrl := coordinator.NewControl(tInfo, s1Info)

	w1 := allocation.NewWorker[location.InstanceID, model.Instance](
		"worker1",
		model.NewInstance(location.NewInstance(location.New("centralus", "unknown")), ""),
	)

	w2 := allocation.NewWorker[location.InstanceID, model.Instance](
		"worker2",
		model.NewInstance(location.NewInstance(location.New("northcentralus", "unknown")), ""),
	)

	w3 := allocation.NewWorker[location.InstanceID, model.Instance](
		"worker3",
		model.NewInstance(location.NewInstance(location.New("eastus2", "unknown")), ""),
	)

	w4 := allocation.NewWorker[location.InstanceID, model.Instance](
		"worker4",
		model.NewInstance(location.NewInstance(location.New("us-central1", "unknown")), ""),
	)

	_, ok := ctrl.TryPlace(w1, allocation.Work[model.Shard, location.Location]{Unit: model.Shard{
		Domain: d1.Name(),
	}})
	assert.False(t, ok)

	_, ok = ctrl.TryPlace(w1, allocation.Work[model.Shard, location.Location]{Unit: model.Shard{
		Domain: d2.Name(),
	}})
	assert.False(t, ok)

	_, ok = ctrl.TryPlace(w1, allocation.Work[model.Shard, location.Location]{Unit: model.Shard{
		Domain: d3.Name(),
	}})
	assert.False(t, ok)

	_, ok = ctrl.TryPlace(w2, allocation.Work[model.Shard, location.Location]{Unit: model.Shard{
		Domain: d1.Name(),
	}})
	assert.False(t, ok)

	_, ok = ctrl.TryPlace(w2, allocation.Work[model.Shard, location.Location]{Unit: model.Shard{
		Domain: d2.Name(),
	}})
	assert.False(t, ok)

	_, ok = ctrl.TryPlace(w2, allocation.Work[model.Shard, location.Location]{Unit: model.Shard{
		Domain: d3.Name(),
	}})
	assert.False(t, ok)

	_, ok = ctrl.TryPlace(w3, allocation.Work[model.Shard, location.Location]{Unit: model.Shard{
		Domain: d1.Name(),
	}})
	assert.True(t, ok)

	_, ok = ctrl.TryPlace(w3, allocation.Work[model.Shard, location.Location]{Unit: model.Shard{
		Domain: d2.Name(),
	}})
	assert.False(t, ok)

	_, ok = ctrl.TryPlace(w3, allocation.Work[model.Shard, location.Location]{Unit: model.Shard{
		Domain: d3.Name(),
	}})
	assert.False(t, ok)

	_, ok = ctrl.TryPlace(w4, allocation.Work[model.Shard, location.Location]{Unit: model.Shard{
		Domain: d1.Name(),
	}})
	assert.True(t, ok)

	_, ok = ctrl.TryPlace(w4, allocation.Work[model.Shard, location.Location]{Unit: model.Shard{
		Domain: d2.Name(),
	}})
	assert.True(t, ok)

	_, ok = ctrl.TryPlace(w4, allocation.Work[model.Shard, location.Location]{Unit: model.Shard{
		Domain: d3.Name(),
	}})
	assert.False(t, ok)
}
