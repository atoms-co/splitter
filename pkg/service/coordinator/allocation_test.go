package coordinator_test

import (
	"go.atoms.co/splitter/lib/service/location"
	"go.atoms.co/lib/testing/assertx"
	"go.atoms.co/lib/testing/mockclock"
	"go.atoms.co/splitter/pkg/allocation"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pkg/service/coordinator"
	"go.atoms.co/splitter/testing/prefab"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestControl(t *testing.T) {
	t1, err := model.NewTenant("tenant1", time.Time{}, model.WithTenantOperational(
		model.NewTenantOperational(model.WithTenantOperationalBannedRegions("centralus")),
	))
	require.NoError(t, err)

	s1, err := model.NewService(model.QualifiedServiceName{Tenant: t1.Name(), Service: "service1"}, time.Time{}, model.WithServiceOperational(
		model.NewServiceOperational(model.WithServiceOperationalBannedRegions("northcentralus")),
	))
	require.NoError(t, err)

	d1, err := model.NewDomain(model.QualifiedDomainName{Service: s1.Name(), Domain: "domain1"}, model.Regional, time.Time{}, model.WithDomainConfig(
		model.NewDomainConfig(model.WithDomainShardingPolicy(model.NewShardingPolicy(4))),
	))
	require.NoError(t, err)

	d2, err := model.NewDomain(model.QualifiedDomainName{Service: s1.Name(), Domain: "domain2"}, model.Regional, time.Time{},
		model.WithDomainConfig(
			model.NewDomainConfig(model.WithDomainShardingPolicy(model.NewShardingPolicy(4))),
		),
		model.WithDomainOperational(
			model.NewDomainOperational(model.WithDomainOperationalBannedRegions("eastus2")),
		),
	)
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

func TestColocation(t *testing.T) {
	a := uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
	b := uuid.MustParse("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
	c := uuid.MustParse("cccccccc-cccc-cccc-cccc-cccccccccccc")

	d1 := prefab.QDN("t/s/d1")
	d2 := prefab.QDN("t/s/d2")

	ab := model.Shard{Domain: d1, Type: model.Global, From: model.Key(a), To: model.Key(b)}
	bc := model.Shard{Domain: d2, Type: model.Global, From: model.Key(b), To: model.Key(c)}
	ac := model.Shard{Domain: d2, Type: model.Global, From: model.Key(a), To: model.Key(c)}

	cl := mockclock.NewUnsynchronized()

	tests := []struct {
		shard1, shard2 model.Shard
		antiAffinity   bool
		penalized      bool
	}{
		{ab, bc, true, false},
		{ab, ac, true, true},
		{ab, ac, false, false},
	}

	for _, tt := range tests {
		var dom1 model.Domain
		if tt.antiAffinity {
			dom1, _ = model.NewDomain(d1, model.Global, cl.Now(), model.WithDomainConfig(model.NewDomainConfig(model.WithDomainAntiAffinity(d2.Domain))))
		} else {
			dom1, _ = model.NewDomain(d1, model.Global, cl.Now())
		}
		dom2, _ := model.NewDomain(d2, model.Global, cl.Now())

		service, _ := model.NewService(serviceName, cl.Now())
		info := model.NewServiceInfoEx(model.NewServiceInfo(service, 1, cl.Now()), []model.Domain{dom1, dom2})
		worker := allocation.NewWorker[model.ConsumerID, model.Consumer]("worker-id", prefab.Instance1)
		affinity := coordinator.NewAffinity(info)
		load := affinity.Colocate(worker, map[model.Shard]coordinator.Work{
			tt.shard1: {Unit: tt.shard1},
			tt.shard2: {Unit: tt.shard2},
		})
		assertx.Equal(t, load[tt.shard1] > 0, tt.penalized)
		assertx.Equal(t, load[tt.shard2], 0) // Shard 2 is never penalized
	}
}
