package coordinator_test

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.atoms.co/splitter/lib/service/location"
	"go.atoms.co/lib/testing/assertx"
	"go.atoms.co/lib/testing/mockclock"
	"go.atoms.co/splitter/pkg/allocation"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pkg/service/coordinator"
	"go.atoms.co/splitter/pb"
	"go.atoms.co/splitter/testing/prefab"
)

func TestNamedShards(t *testing.T) {
	cl := mockclock.NewUnsynchronized()

	t1, err := model.NewTenant("tenant1", time.Time{})
	require.NoError(t, err)

	s1, err := model.NewService(model.QualifiedServiceName{Tenant: t1.Name(), Service: "service1"}, time.Time{})
	require.NoError(t, err)

	d1, err := model.NewDomain(model.QualifiedDomainName{Service: s1.Name(), Domain: "domain1"}, model.Regional, time.Time{}, model.WithDomainConfig(
		model.NewDomainConfig(
			model.WithDomainShardingPolicy(model.NewShardingPolicy(4)),
			model.WithDomainRegions("centralus"),
		),
	))
	require.NoError(t, err)

	d2, err := model.NewDomain(model.QualifiedDomainName{Service: s1.Name(), Domain: "domain2"}, model.Regional, time.Time{},
		model.WithDomainConfig(
			model.NewDomainConfig(
				model.WithDomainShardingPolicy(model.NewShardingPolicy(4)),
				model.WithDomainRegions("northcentralus"),
			),
		))
	require.NoError(t, err)

	canaryKeys := []model.QualifiedDomainKey{
		model.MustParseQualifiedDomainKey(&public_v1.QualifiedDomainKey{
			Domain: d1.Name().ToProto(),
			Key: &public_v1.DomainKey{
				Region: "centralus",
				Key:    "b188ea31-f889-4ce5-9fc9-77fda8ab5c83",
			},
		}),
		model.MustParseQualifiedDomainKey(&public_v1.QualifiedDomainKey{
			Domain: d2.Name().ToProto(),
			Key: &public_v1.DomainKey{
				Region: "northcentralus",
				Key:    "c5b6882b-3dee-4e21-874d-14824bc86c82",
			},
		}),
	}

	namedShards := []model.Shard{
		{
			Domain: d1.Name(),
			Type:   model.Regional,
			Region: "centralus",
			To:     model.MustParseKey("c0000000-0000-0000-0000-000000000000"),
			From:   model.MustParseKey("b0000000-0000-0000-0000-000000000000"),
		},
		{
			Domain: d2.Name(),
			Type:   model.Regional,
			Region: "northcentralus",
			To:     model.MustParseKey("d0000000-0000-0000-0000-000000000000"),
			From:   model.MustParseKey("c0000000-0000-0000-0000-000000000000"),
		},
	}

	ctrl := coordinator.NewNamedShards(namedShards...)

	w1 := allocation.NewWorker[location.InstanceID, *coordinator.Consumer](
		"worker1",
		coordinator.NewConsumer(
			model.NewInstance(location.NewInstance(location.New("centralus", "unknown")), ""),
			cl.Now(),
			coordinator.WithKeys(canaryKeys[0]),
		),
	)

	w2 := allocation.NewWorker[location.InstanceID, *coordinator.Consumer](
		"worker2",
		coordinator.NewConsumer(
			model.NewInstance(location.NewInstance(location.New("northcentralus", "unknown")), ""),
			cl.Now(),
			coordinator.WithKeys(canaryKeys[1]),
		),
	)

	w3 := allocation.NewWorker[location.InstanceID, *coordinator.Consumer](
		"worker3",
		coordinator.NewConsumer(
			model.NewInstance(location.NewInstance(location.New("centralus", "unknown")), ""),
			cl.Now(),
		),
	)

	load, ok := ctrl.TryPlace(w1, allocation.Work[model.Shard, location.Location]{Unit: namedShards[0]})
	assert.True(t, ok)
	assert.Equal(t, allocation.Load(0), load)

	_, ok = ctrl.TryPlace(w1, allocation.Work[model.Shard, location.Location]{Unit: model.Shard{
		Domain: d1.Name(),
		Type:   model.Regional,
		Region: "centralus",
		To:     model.MustParseKey("d0000000-0000-0000-0000-000000000000"),
		From:   model.MustParseKey("c0000000-0000-0000-0000-000000000000"),
	}})
	assert.False(t, ok)

	load, ok = ctrl.TryPlace(w2, allocation.Work[model.Shard, location.Location]{Unit: namedShards[1]})
	assert.True(t, ok)
	assert.Equal(t, allocation.Load(0), load)

	_, ok = ctrl.TryPlace(w2, allocation.Work[model.Shard, location.Location]{Unit: model.Shard{
		Domain: d2.Name(),
		Type:   model.Regional,
		Region: "northcentralus",
		To:     model.MustParseKey("e0000000-0000-0000-0000-000000000000"),
		From:   model.MustParseKey("d0000000-0000-0000-0000-000000000000"),
	}})
	assert.False(t, ok)

	load, ok = ctrl.TryPlace(w3, allocation.Work[model.Shard, location.Location]{Unit: namedShards[0]})
	assert.True(t, ok)
	assert.Equal(t, allocation.Load(20), load)

	load, ok = ctrl.TryPlace(w3, allocation.Work[model.Shard, location.Location]{Unit: namedShards[1]})
	assert.True(t, ok)
	assert.Equal(t, allocation.Load(20), load)
}

func TestDomainState(t *testing.T) {
	cl := mockclock.NewUnsynchronized()

	t1, err := model.NewTenant("tenant1", time.Time{})
	require.NoError(t, err)

	s1, err := model.NewService(model.QualifiedServiceName{Tenant: t1.Name(), Service: "service1"}, time.Time{})
	require.NoError(t, err)

	d1, err := model.NewDomain(model.QualifiedDomainName{Service: s1.Name(), Domain: "domain1"}, model.Regional, time.Time{}, model.WithDomainConfig(
		model.NewDomainConfig(model.WithDomainShardingPolicy(model.NewShardingPolicy(4))),
	))
	require.NoError(t, err)

	d2, err := model.NewDomain(model.QualifiedDomainName{Service: s1.Name(), Domain: "domain2"}, model.Regional, time.Time{},
		model.WithDomainState(model.DomainSuspended),
		model.WithDomainConfig(
			model.NewDomainConfig(model.WithDomainShardingPolicy(model.NewShardingPolicy(4))),
		))
	require.NoError(t, err)

	tInfo := model.NewTenantInfo(t1, 1, time.Time{})
	s1Info := model.NewServiceInfoEx(model.NewServiceInfo(s1, 1, time.Time{}), []model.Domain{d1, d2})

	ctrl := coordinator.NewDomainState(tInfo, s1Info)

	w1 := allocation.NewWorker[location.InstanceID, *coordinator.Consumer](
		"worker1",
		coordinator.NewConsumer(model.NewInstance(location.NewInstance(location.New("centralus", "unknown")), ""), cl.Now()),
	)

	_, ok := ctrl.TryPlace(w1, allocation.Work[model.Shard, location.Location]{Unit: model.Shard{
		Domain: d1.Name(),
	}})
	assert.True(t, ok)

	_, ok = ctrl.TryPlace(w1, allocation.Work[model.Shard, location.Location]{Unit: model.Shard{
		Domain: d2.Name(),
	}})
	assert.False(t, ok)
}

func TestBannedWorkerRegion(t *testing.T) {
	cl := mockclock.NewUnsynchronized()

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

	tInfo := model.NewTenantInfo(t1, 1, time.Time{})
	s1Info := model.NewServiceInfoEx(model.NewServiceInfo(s1, 1, time.Time{}), []model.Domain{d1, d2})

	ctrl := coordinator.NewBannedWorkerRegion(tInfo, s1Info)

	w1 := allocation.NewWorker[location.InstanceID, *coordinator.Consumer](
		"worker1",
		coordinator.NewConsumer(model.NewInstance(location.NewInstance(location.New("centralus", "unknown")), ""), cl.Now()),
	)

	w2 := allocation.NewWorker[location.InstanceID, *coordinator.Consumer](
		"worker2",
		coordinator.NewConsumer(model.NewInstance(location.NewInstance(location.New("northcentralus", "unknown")), ""), cl.Now()),
	)

	w3 := allocation.NewWorker[location.InstanceID, *coordinator.Consumer](
		"worker3",
		coordinator.NewConsumer(model.NewInstance(location.NewInstance(location.New("eastus2", "unknown")), ""), cl.Now()),
	)

	w4 := allocation.NewWorker[location.InstanceID, *coordinator.Consumer](
		"worker4",
		coordinator.NewConsumer(model.NewInstance(location.NewInstance(location.New("us-central1", "unknown")), ""), cl.Now()),
	)

	_, ok := ctrl.TryPlace(w1, allocation.Work[model.Shard, location.Location]{Unit: model.Shard{
		Domain: d1.Name(),
	}})
	assert.False(t, ok)

	_, ok = ctrl.TryPlace(w1, allocation.Work[model.Shard, location.Location]{Unit: model.Shard{
		Domain: d2.Name(),
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

	_, ok = ctrl.TryPlace(w3, allocation.Work[model.Shard, location.Location]{Unit: model.Shard{
		Domain: d1.Name(),
	}})
	assert.True(t, ok)

	_, ok = ctrl.TryPlace(w3, allocation.Work[model.Shard, location.Location]{Unit: model.Shard{
		Domain: d2.Name(),
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

}

func TestRegionAffinity(t *testing.T) {
	cl := mockclock.NewUnsynchronized()

	s1, err := model.NewService(model.QualifiedServiceName{Tenant: "tenant1", Service: "service1"}, time.Time{}, model.WithServiceConfig(
		model.NewServiceConfig(model.WithLocalityOverrides(map[location.Region]location.Region{
			"us-central1": "centralus",
		})),
	))
	require.NoError(t, err)
	s1Info := model.NewServiceInfoEx(model.NewServiceInfo(s1, 1, time.Time{}), nil)

	affinity := coordinator.NewRegionAffinity(s1Info)

	w1 := allocation.NewWorker[location.InstanceID, *coordinator.Consumer](
		"worker1",
		coordinator.NewConsumer(model.NewInstance(location.NewInstance(location.New("centralus", "unknown")), ""), cl.Now()),
	)

	penalty, ok := affinity.TryPlace(w1, allocation.Work[model.Shard, location.Location]{
		Unit: model.Shard{},
		Data: location.New("centralus", ""),
	})
	assert.True(t, ok)
	assert.Equal(t, allocation.Load(0), penalty)

	penalty, ok = affinity.TryPlace(w1, allocation.Work[model.Shard, location.Location]{
		Unit: model.Shard{},
		Data: location.New("us-central1", ""),
	})
	assert.True(t, ok)
	assert.Equal(t, allocation.Load(0), penalty)

	penalty, ok = affinity.TryPlace(w1, allocation.Work[model.Shard, location.Location]{
		Unit: model.Shard{},
		Data: location.New("northcentralus", ""),
	})
	assert.True(t, ok)
	assert.Equal(t, allocation.Load(20), penalty)
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
		var err error
		if tt.antiAffinity {
			dom1, err = model.NewDomain(d1, model.Global, cl.Now(), model.WithDomainConfig(
				model.NewDomainConfig(
					model.WithDomainShardingPolicy(model.NewShardingPolicy(1)),
					model.WithDomainAntiAffinity(d2.Domain),
				),
			))
			assert.NoError(t, err)
		} else {
			dom1, err = model.NewDomain(d1, model.Global, cl.Now(), model.WithDomainConfig(
				model.NewDomainConfig(
					model.WithDomainShardingPolicy(model.NewShardingPolicy(1)),
				),
			))
			assert.NoError(t, err)
		}
		dom2, err := model.NewDomain(d2, model.Global, cl.Now(), model.WithDomainConfig(
			model.NewDomainConfig(
				model.WithDomainShardingPolicy(model.NewShardingPolicy(1)),
			),
		))
		assert.NoError(t, err)

		service, err := model.NewService(serviceName, cl.Now())
		assert.NoError(t, err)

		info := model.NewServiceInfoEx(model.NewServiceInfo(service, 1, cl.Now()), []model.Domain{dom1, dom2})
		worker := allocation.NewWorker[model.ConsumerID, *coordinator.Consumer]("worker-id", coordinator.NewConsumer(prefab.Instance1, cl.Now()))
		affinity := coordinator.NewAntiAffinity(info)
		load := affinity.Colocate(worker, map[model.Shard]coordinator.Work{
			tt.shard1: {Unit: tt.shard1},
			tt.shard2: {Unit: tt.shard2},
		})
		assertx.Equal(t, load[tt.shard1] > 0, tt.penalized)
		assertx.Equal(t, load[tt.shard2], 0) // Shard 2 is never penalized
	}
}
