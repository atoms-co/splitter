package coordinator

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.atoms.co/splitter/lib/service/location"
	"go.atoms.co/lib/testing/assertx"
	"go.atoms.co/slicex"
	"go.atoms.co/lib/uuidx"
	"go.atoms.co/splitter/pkg/allocation"
	"go.atoms.co/splitter/pkg/model"
	splitterpb "go.atoms.co/splitter/pb"
	"go.atoms.co/splitter/testing/prefab"
)

func TestNamedShards(t *testing.T) {
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

	canaryKeys := []qualifiedDomainKeyWithName{
		{
			key: model.MustParseQualifiedDomainKey(&splitterpb.QualifiedDomainKey{
				Domain: d1.Name().ToProto(),
				Key: &splitterpb.DomainKey{
					Region: "centralus",
					Key:    "b188ea31-f889-4ce5-9fc9-77fda8ab5c83",
				},
			}),
		},
		{
			key: model.MustParseQualifiedDomainKey(&splitterpb.QualifiedDomainKey{
				Domain: d2.Name().ToProto(),
				Key: &splitterpb.DomainKey{
					Region: "northcentralus",
					Key:    "c5b6882b-3dee-4e21-874d-14824bc86c82",
				},
			}),
		},
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

	ctrl := NewNamedShards(namedShards...)

	w1 := allocation.NewWorker[location.InstanceID, *Consumer](
		"worker1",
		NewConsumer(
			model.NewInstance(location.NewInstance(location.New("centralus", "unknown")), ""),
			time.Now(),
			withKeys(canaryKeys[0]),
		),
	)

	w2 := allocation.NewWorker[location.InstanceID, *Consumer](
		"worker2",
		NewConsumer(
			model.NewInstance(location.NewInstance(location.New("northcentralus", "unknown")), ""),
			time.Now(),
			withKeys(canaryKeys[1]),
		),
	)

	w3 := allocation.NewWorker[location.InstanceID, *Consumer](
		"worker3",
		NewConsumer(
			model.NewInstance(location.NewInstance(location.New("centralus", "unknown")), ""),
			time.Now(),
		),
	)

	load, ok := ctrl.TryPlace(w1, Work{Unit: namedShards[0]})
	assert.True(t, ok)
	assert.Equal(t, allocation.Load(0), load)

	_, ok = ctrl.TryPlace(w1, allocation.Work[model.Shard, []location.Location]{Unit: model.Shard{
		Domain: d1.Name(),
		Type:   model.Regional,
		Region: "centralus",
		To:     model.MustParseKey("d0000000-0000-0000-0000-000000000000"),
		From:   model.MustParseKey("c0000000-0000-0000-0000-000000000000"),
	}})
	assert.False(t, ok)

	load, ok = ctrl.TryPlace(w2, Work{Unit: namedShards[1]})
	assert.True(t, ok)
	assert.Equal(t, allocation.Load(0), load)

	_, ok = ctrl.TryPlace(w2, Work{Unit: model.Shard{
		Domain: d2.Name(),
		Type:   model.Regional,
		Region: "northcentralus",
		To:     model.MustParseKey("e0000000-0000-0000-0000-000000000000"),
		From:   model.MustParseKey("d0000000-0000-0000-0000-000000000000"),
	}})
	assert.False(t, ok)

	load, ok = ctrl.TryPlace(w3, Work{Unit: namedShards[0]})
	assert.True(t, ok)
	assert.Equal(t, allocation.Load(20), load)

	load, ok = ctrl.TryPlace(w3, Work{Unit: namedShards[1]})
	assert.True(t, ok)
	assert.Equal(t, allocation.Load(20), load)
}

func TestDomainState(t *testing.T) {
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

	ctrl := NewDomainState(tInfo, s1Info)

	w1 := allocation.NewWorker[location.InstanceID, *Consumer](
		"worker1",
		NewConsumer(model.NewInstance(location.NewInstance(location.New("centralus", "unknown")), ""), time.Now()),
	)

	_, ok := ctrl.TryPlace(w1, Work{Unit: model.Shard{
		Domain: d1.Name(),
	}})
	assert.True(t, ok)

	_, ok = ctrl.TryPlace(w1, Work{Unit: model.Shard{
		Domain: d2.Name(),
	}})
	assert.False(t, ok)
}

func TestBannedWorkerRegion(t *testing.T) {
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

	ctrl := NewBannedWorkerRegion(tInfo, s1Info)

	w1 := allocation.NewWorker[location.InstanceID, *Consumer](
		"worker1",
		NewConsumer(model.NewInstance(location.NewInstance(location.New("centralus", "unknown")), ""), time.Now()),
	)

	w2 := allocation.NewWorker[location.InstanceID, *Consumer](
		"worker2",
		NewConsumer(model.NewInstance(location.NewInstance(location.New("northcentralus", "unknown")), ""), time.Now()),
	)

	w3 := allocation.NewWorker[location.InstanceID, *Consumer](
		"worker3",
		NewConsumer(model.NewInstance(location.NewInstance(location.New("eastus2", "unknown")), ""), time.Now()),
	)

	w4 := allocation.NewWorker[location.InstanceID, *Consumer](
		"worker4",
		NewConsumer(model.NewInstance(location.NewInstance(location.New("us-central1", "unknown")), ""), time.Now()),
	)

	_, ok := ctrl.TryPlace(w1, Work{Unit: model.Shard{
		Domain: d1.Name(),
	}})
	assert.False(t, ok)

	_, ok = ctrl.TryPlace(w1, Work{Unit: model.Shard{
		Domain: d2.Name(),
	}})
	assert.False(t, ok)

	_, ok = ctrl.TryPlace(w2, Work{Unit: model.Shard{
		Domain: d1.Name(),
	}})
	assert.False(t, ok)

	_, ok = ctrl.TryPlace(w2, Work{Unit: model.Shard{
		Domain: d2.Name(),
	}})
	assert.False(t, ok)

	_, ok = ctrl.TryPlace(w3, Work{Unit: model.Shard{
		Domain: d1.Name(),
	}})
	assert.True(t, ok)

	_, ok = ctrl.TryPlace(w3, Work{Unit: model.Shard{
		Domain: d2.Name(),
	}})
	assert.False(t, ok)

	_, ok = ctrl.TryPlace(w4, Work{Unit: model.Shard{
		Domain: d1.Name(),
	}})
	assert.True(t, ok)

	_, ok = ctrl.TryPlace(w4, Work{Unit: model.Shard{
		Domain: d2.Name(),
	}})
	assert.True(t, ok)

}

func TestRegionAffinity(t *testing.T) {
	s1, err := model.NewService(model.QualifiedServiceName{Tenant: "tenant1", Service: "service1"}, time.Time{}, model.WithServiceConfig(
		model.NewServiceConfig(model.WithLocalityOverrides(map[location.Region]location.Region{
			"us-central1": "centralus",
		})),
	))
	require.NoError(t, err)
	s1Info := model.NewServiceInfoEx(model.NewServiceInfo(s1, 1, time.Time{}), nil)

	affinity := NewRegionAffinity(s1Info)

	w1 := allocation.NewWorker[location.InstanceID, *Consumer](
		"worker1",
		NewConsumer(model.NewInstance(location.NewInstance(location.New("centralus", "unknown")), ""), time.Now()),
	)

	penalty, ok := affinity.TryPlace(w1, Work{
		Unit: model.Shard{},
		Data: slicex.New(location.New("centralus", "")),
	})
	assert.True(t, ok)
	assert.Equal(t, allocation.Load(0), penalty)

	penalty, ok = affinity.TryPlace(w1, Work{
		Unit: model.Shard{},
		Data: slicex.New(location.New("us-central1", "")),
	})
	assert.True(t, ok)
	assert.Equal(t, allocation.Load(0), penalty)

	penalty, ok = affinity.TryPlace(w1, Work{
		Unit: model.Shard{},
		Data: slicex.New(location.New("northcentralus", "")),
	})
	assert.True(t, ok)
	assert.Equal(t, allocation.Load(20), penalty)
}

func TestMultiRegionAffinity(t *testing.T) {
	s1, err := model.NewService(model.QualifiedServiceName{Tenant: "tenant1", Service: "service1"}, time.Time{}, model.WithServiceConfig(
		model.NewServiceConfig()),
	)
	require.NoError(t, err)
	affinity := NewRegionAffinity(model.NewServiceInfoEx(model.NewServiceInfo(s1, 1, time.Time{}), nil))

	w1 := allocation.NewWorker[location.InstanceID, *Consumer](
		"worker1",
		NewConsumer(model.NewInstance(location.NewInstance(location.New("region1", "unknown")), ""), time.Now()),
	)

	w2 := allocation.NewWorker[location.InstanceID, *Consumer](
		"worker2",
		NewConsumer(model.NewInstance(location.NewInstance(location.New("region2", "unknown")), ""), time.Now()),
	)

	w3 := allocation.NewWorker[location.InstanceID, *Consumer](
		"worker3",
		NewConsumer(model.NewInstance(location.NewInstance(location.New("region3", "unknown")), ""), time.Now()),
	)

	noPrefWork := makeWork()
	singlePrefWork := makeWork("region1")
	multiPrefWork := makeWork("region1", "region2")

	tests := []struct {
		name     string
		work     Work
		worker   Worker
		expected allocation.Load
	}{
		{
			name:     "No region preference, w1 no penalty",
			work:     noPrefWork,
			worker:   w1,
			expected: 0,
		},
		{
			name:     "No region preference, w2 no penalty",
			work:     noPrefWork,
			worker:   w2,
			expected: 0,
		},
		{
			name:     "No region preference, w2 no penalty",
			work:     noPrefWork,
			worker:   w2,
			expected: 0,
		},
		{
			name:     "Region1 preference w1 no penalty",
			worker:   w1,
			work:     singlePrefWork,
			expected: 0,
		},
		{
			name:     "Region1 preference w2 penalized",
			worker:   w2,
			work:     singlePrefWork,
			expected: 20,
		},
		{
			name:     "Region1 preference w3 penalized",
			worker:   w1,
			work:     singlePrefWork,
			expected: 0,
		},
		{
			name:     "region1 and region2 preference w1 no penalty",
			worker:   w1,
			work:     multiPrefWork,
			expected: 0,
		},
		{
			name:     "region1 and region2 preference w2 no penalty",
			worker:   w2,
			work:     multiPrefWork,
			expected: 0,
		},
		{
			name:     "region1 and region2 preference w3 penalized",
			worker:   w3,
			work:     multiPrefWork,
			expected: 20,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			load, ok := affinity.TryPlace(tt.worker, tt.work)
			assert.True(t, ok)
			assert.Equal(t, tt.expected, load)
		})
	}
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
			dom1, err = model.NewDomain(d1, model.Global, time.Now(), model.WithDomainConfig(
				model.NewDomainConfig(
					model.WithDomainShardingPolicy(model.NewShardingPolicy(1)),
					model.WithDomainAntiAffinity(d2.Domain),
				),
			))
			assert.NoError(t, err)
		} else {
			dom1, err = model.NewDomain(d1, model.Global, time.Now(), model.WithDomainConfig(
				model.NewDomainConfig(
					model.WithDomainShardingPolicy(model.NewShardingPolicy(1)),
				),
			))
			assert.NoError(t, err)
		}
		dom2, err := model.NewDomain(d2, model.Global, time.Now(), model.WithDomainConfig(
			model.NewDomainConfig(
				model.WithDomainShardingPolicy(model.NewShardingPolicy(1)),
			),
		))
		assert.NoError(t, err)

		service, err := model.NewService(serviceName, time.Now())
		assert.NoError(t, err)

		info := model.NewServiceInfoEx(model.NewServiceInfo(service, 1, time.Now()), []model.Domain{dom1, dom2})
		worker := allocation.NewWorker[model.ConsumerID, *Consumer]("worker-id", NewConsumer(prefab.Instance1, time.Now()))
		affinity := NewAntiAffinity(info)
		load := affinity.Colocate(worker, map[model.Shard]Work{
			tt.shard1: {Unit: tt.shard1},
			tt.shard2: {Unit: tt.shard2},
		})
		assertx.Equal(t, load[tt.shard1] > 0, tt.penalized)
		assertx.Equal(t, load[tt.shard2], 0) // Shard 2 is never penalized
	}
}

func TestShardSplitting(t *testing.T) {
	tenant, err := model.NewTenant("tenant1", time.Time{})
	require.NoError(t, err)

	service, err := model.NewService(model.QualifiedServiceName{Tenant: tenant.Name(), Service: "service1"}, time.Time{})
	require.NoError(t, err)

	tests := []struct {
		name         string
		customShards []model.ShardingPolicyShard
		expected     []uuidx.Range
	}{
		{
			name:         "default shards",
			customShards: nil,
			expected: []uuidx.Range{
				makeRange("00000000-0000-0000-0000-000000000000", "80000000-0000-0000-0000-000000000000"),
				makeRange("80000000-0000-0000-0000-000000000000", "ffffffff-ffff-ffff-ffff-ffffffffffff"),
			},
		},
		{
			name: "three equal shards",
			customShards: []model.ShardingPolicyShard{
				{
					From: model.MustParseKey("00000000-0000-0000-0000-000000000000"),
					To:   model.MustParseKey("40000000-0000-0000-0000-000000000000"),
				},
				{
					From: model.MustParseKey("40000000-0000-0000-0000-000000000000"),
					To:   model.MustParseKey("80000000-0000-0000-0000-000000000000"),
				},
				{
					From: model.MustParseKey("80000000-0000-0000-0000-000000000000"),
					To:   model.MustParseKey("ffffffff-ffff-ffff-ffff-ffffffffffff"),
				},
			},
			expected: []uuidx.Range{
				makeRange("00000000-0000-0000-0000-000000000000", "40000000-0000-0000-0000-000000000000"),
				makeRange("40000000-0000-0000-0000-000000000000", "80000000-0000-0000-0000-000000000000"),
				makeRange("80000000-0000-0000-0000-000000000000", "ffffffff-ffff-ffff-ffff-ffffffffffff"),
			},
		},
		{
			name: "overlapping shards",
			customShards: []model.ShardingPolicyShard{
				{
					From: model.MustParseKey("00000000-0000-0000-0000-000000000000"),
					To:   model.MustParseKey("60000000-0000-0000-0000-000000000000"),
				},
				{
					From: model.MustParseKey("40000000-0000-0000-0000-000000000000"),
					To:   model.MustParseKey("ffffffff-ffff-ffff-ffff-ffffffffffff"),
				},
			},
			expected: []uuidx.Range{
				makeRange("00000000-0000-0000-0000-000000000000", "80000000-0000-0000-0000-000000000000"),
				makeRange("80000000-0000-0000-0000-000000000000", "ffffffff-ffff-ffff-ffff-ffffffffffff"),
			},
		},
		{
			name: "non overlapping shards",
			customShards: []model.ShardingPolicyShard{
				{
					From: model.MustParseKey("00000000-0000-0000-0000-000000000000"),
					To:   model.MustParseKey("40000000-0000-0000-0000-000000000000"),
				},
				{
					From: model.MustParseKey("50000000-0000-0000-0000-000000000000"),
					To:   model.MustParseKey("ffffffff-ffff-ffff-ffff-ffffffffffff"),
				},
			},
			expected: []uuidx.Range{
				makeRange("00000000-0000-0000-0000-000000000000", "40000000-0000-0000-0000-000000000000"),
				makeRange("40000000-0000-0000-0000-000000000000", "50000000-0000-0000-0000-000000000000"),
				makeRange("50000000-0000-0000-0000-000000000000", "ffffffff-ffff-ffff-ffff-ffffffffffff"),
			},
		},
		{
			name: "UnevenCustomShards",
			customShards: []model.ShardingPolicyShard{
				{
					From: model.MustParseKey("00000000-0000-0000-0000-000000000000"),
					To:   model.MustParseKey("20000000-0000-0000-0000-000000000000"),
				},
				{
					From: model.MustParseKey("20000000-0000-0000-0000-000000000000"),
					To:   model.MustParseKey("40000000-0000-0000-0000-000000000000"),
				},
				{
					From: model.MustParseKey("40000000-0000-0000-0000-000000000000"),
					To:   model.MustParseKey("a0000000-0000-0000-0000-000000000000"),
				},
				{
					From: model.MustParseKey("a0000000-0000-0000-0000-000000000000"),
					To:   model.MustParseKey("ffffffff-ffff-ffff-ffff-ffffffffffff"),
				},
			},
			expected: []uuidx.Range{
				makeRange("00000000-0000-0000-0000-000000000000", "20000000-0000-0000-0000-000000000000"),
				makeRange("20000000-0000-0000-0000-000000000000", "40000000-0000-0000-0000-000000000000"),
				makeRange("40000000-0000-0000-0000-000000000000", "a0000000-0000-0000-0000-000000000000"),
				makeRange("a0000000-0000-0000-0000-000000000000", "ffffffff-ffff-ffff-ffff-ffffffffffff"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			name := model.QualifiedDomainName{Service: service.Name(), Domain: "test-domain"}
			sp := model.NewShardingPolicy(2, model.WithShardingPolicyShards(tt.customShards))
			opts := model.WithDomainConfig(model.NewDomainConfig(model.WithDomainShardingPolicy(sp)))
			domain, err := model.NewDomain(name, model.Global, time.Now(), opts)
			require.NoError(t, err)

			actual := findShards(domain)

			assertx.Equal(t, len(tt.expected), len(actual), "expected %d shards, got %d", len(tt.expected), len(actual))

			assertSameShards(t, tt.expected, actual)
		})
	}
}

func TestFindShardsForRegion(t *testing.T) {
	tenant, err := model.NewTenant("tenant1", time.Time{})
	require.NoError(t, err)

	service, err := model.NewService(model.QualifiedServiceName{Tenant: tenant.Name(), Service: "service1"}, time.Time{})
	require.NoError(t, err)

	tests := []struct {
		name          string
		targetShards  int
		customShards  []model.ShardingPolicyShard
		regionResults map[model.Region][]uuidx.Range
	}{
		{
			name:         "multiple regions with non-overlapping custom shards",
			targetShards: 2,
			customShards: []model.ShardingPolicyShard{
				{
					From:   model.MustParseKey("00000000-0000-0000-0000-000000000000"),
					To:     model.MustParseKey("40000000-0000-0000-0000-000000000000"),
					Region: "us",
				},
				{
					From:   model.MustParseKey("40000000-0000-0000-0000-000000000000"),
					To:     model.MustParseKey("80000000-0000-0000-0000-000000000000"),
					Region: "eu",
				},
				{
					From:   model.MustParseKey("80000000-0000-0000-0000-000000000000"),
					To:     model.MustParseKey("ffffffff-ffff-ffff-ffff-ffffffffffff"),
					Region: "asia",
				},
			},
			regionResults: map[model.Region][]uuidx.Range{
				"us": {
					makeRange("00000000-0000-0000-0000-000000000000", "40000000-0000-0000-0000-000000000000"),
					makeRange("40000000-0000-0000-0000-000000000000", "80000000-0000-0000-0000-000000000000"),
					makeRange("80000000-0000-0000-0000-000000000000", "ffffffff-ffff-ffff-ffff-ffffffffffff"),
				},
				"eu": {
					makeRange("00000000-0000-0000-0000-000000000000", "40000000-0000-0000-0000-000000000000"),
					makeRange("40000000-0000-0000-0000-000000000000", "80000000-0000-0000-0000-000000000000"),
					makeRange("80000000-0000-0000-0000-000000000000", "ffffffff-ffff-ffff-ffff-ffffffffffff"),
				},
				"asia": {
					makeRange("00000000-0000-0000-0000-000000000000", "80000000-0000-0000-0000-000000000000"),
					makeRange("80000000-0000-0000-0000-000000000000", "ffffffff-ffff-ffff-ffff-ffffffffffff"),
				},
			},
		},
		{
			name:         "overlapping shards across regions",
			targetShards: 2,
			customShards: []model.ShardingPolicyShard{
				{
					From:   model.MustParseKey("00000000-0000-0000-0000-000000000000"),
					To:     model.MustParseKey("80000000-0000-0000-0000-000000000000"),
					Region: "us",
				},
				{
					From:   model.MustParseKey("40000000-0000-0000-0000-000000000000"),
					To:     model.MustParseKey("c0000000-0000-0000-0000-000000000000"),
					Region: "eu",
				},
				{
					From:   model.MustParseKey("80000000-0000-0000-0000-000000000000"),
					To:     model.MustParseKey("ffffffff-ffff-ffff-ffff-ffffffffffff"),
					Region: "asia",
				},
			},
			regionResults: map[model.Region][]uuidx.Range{
				"us": {
					makeRange("00000000-0000-0000-0000-000000000000", "80000000-0000-0000-0000-000000000000"),
					makeRange("80000000-0000-0000-0000-000000000000", "ffffffff-ffff-ffff-ffff-ffffffffffff"),
				},
				"eu": {
					makeRange("00000000-0000-0000-0000-000000000000", "40000000-0000-0000-0000-000000000000"),
					makeRange("40000000-0000-0000-0000-000000000000", "c0000000-0000-0000-0000-000000000000"),
					makeRange("c0000000-0000-0000-0000-000000000000", "ffffffff-ffff-ffff-ffff-ffffffffffff"),
				},
				"asia": {
					makeRange("00000000-0000-0000-0000-000000000000", "80000000-0000-0000-0000-000000000000"),
					makeRange("80000000-0000-0000-0000-000000000000", "ffffffff-ffff-ffff-ffff-ffffffffffff"),
				},
			},
		},
		{
			name:         "identical single uuid custom shards across different regions",
			targetShards: 2,
			customShards: []model.ShardingPolicyShard{
				{
					From:   model.MustParseKey("00000000-0000-0000-0000-000000000001"),
					To:     model.MustParseKey("00000000-0000-0000-0000-000000000002"),
					Region: "us",
				},
				{
					From:   model.MustParseKey("00000000-0000-0000-0000-000000000001"),
					To:     model.MustParseKey("00000000-0000-0000-0000-000000000002"),
					Region: "eu",
				},
				{
					From:   model.MustParseKey("00000000-0000-0000-0000-000000000001"),
					To:     model.MustParseKey("00000000-0000-0000-0000-000000000002"),
					Region: "asia",
				},
			},
			regionResults: map[model.Region][]uuidx.Range{
				"us": {
					makeRange("00000000-0000-0000-0000-000000000000", "00000000-0000-0000-0000-000000000001"),
					makeRange("00000000-0000-0000-0000-000000000001", "00000000-0000-0000-0000-000000000002"),
					makeRange("00000000-0000-0000-0000-000000000002", "80000000-0000-0000-0000-000000000000"),
					makeRange("80000000-0000-0000-0000-000000000000", "ffffffff-ffff-ffff-ffff-ffffffffffff"),
				},
				"eu": {
					makeRange("00000000-0000-0000-0000-000000000000", "00000000-0000-0000-0000-000000000001"),
					makeRange("00000000-0000-0000-0000-000000000001", "00000000-0000-0000-0000-000000000002"),
					makeRange("00000000-0000-0000-0000-000000000002", "80000000-0000-0000-0000-000000000000"),
					makeRange("80000000-0000-0000-0000-000000000000", "ffffffff-ffff-ffff-ffff-ffffffffffff"),
				},
				"asia": {
					makeRange("00000000-0000-0000-0000-000000000000", "00000000-0000-0000-0000-000000000001"),
					makeRange("00000000-0000-0000-0000-000000000001", "00000000-0000-0000-0000-000000000002"),
					makeRange("00000000-0000-0000-0000-000000000002", "80000000-0000-0000-0000-000000000000"),
					makeRange("80000000-0000-0000-0000-000000000000", "ffffffff-ffff-ffff-ffff-ffffffffffff"),
				},
			},
		},
		{
			name:         "some regions with custom shards, others with defaults",
			targetShards: 2,
			customShards: []model.ShardingPolicyShard{
				{
					From:   model.MustParseKey("00000000-0000-0000-0000-000000000000"),
					To:     model.MustParseKey("40000000-0000-0000-0000-000000000000"),
					Region: "us",
				},
				{
					From:   model.MustParseKey("40000000-0000-0000-0000-000000000000"),
					To:     model.MustParseKey("ffffffff-ffff-ffff-ffff-ffffffffffff"),
					Region: "us",
				},
			},
			regionResults: map[model.Region][]uuidx.Range{
				"us": {
					makeRange("00000000-0000-0000-0000-000000000000", "40000000-0000-0000-0000-000000000000"),
					makeRange("40000000-0000-0000-0000-000000000000", "ffffffff-ffff-ffff-ffff-ffffffffffff"),
				},
				"eu": {
					makeRange("00000000-0000-0000-0000-000000000000", "80000000-0000-0000-0000-000000000000"),
					makeRange("80000000-0000-0000-0000-000000000000", "ffffffff-ffff-ffff-ffff-ffffffffffff"),
				},
				"asia": {
					makeRange("00000000-0000-0000-0000-000000000000", "80000000-0000-0000-0000-000000000000"),
					makeRange("80000000-0000-0000-0000-000000000000", "ffffffff-ffff-ffff-ffff-ffffffffffff"),
				},
			},
		},
		{
			name:         "complex multi-region configuration with gaps",
			targetShards: 4,
			customShards: []model.ShardingPolicyShard{
				{
					From:   model.MustParseKey("00000000-0000-0000-0000-000000000000"),
					To:     model.MustParseKey("40000000-0000-0000-0000-000000000000"),
					Region: "us",
				},
				{
					From:   model.MustParseKey("80000000-0000-0000-0000-000000000000"),
					To:     model.MustParseKey("c0000000-0000-0000-0000-000000000000"),
					Region: "us",
				},
				{
					From:   model.MustParseKey("40000000-0000-0000-0000-000000000000"),
					To:     model.MustParseKey("80000000-0000-0000-0000-000000000000"),
					Region: "eu",
				},
				{
					From:   model.MustParseKey("c0000000-0000-0000-0000-000000000000"),
					To:     model.MustParseKey("ffffffff-ffff-ffff-ffff-ffffffffffff"),
					Region: "asia",
				},
			},
			regionResults: map[model.Region][]uuidx.Range{
				"us": {
					makeRange("00000000-0000-0000-0000-000000000000", "40000000-0000-0000-0000-000000000000"),
					makeRange("40000000-0000-0000-0000-000000000000", "80000000-0000-0000-0000-000000000000"),
					makeRange("80000000-0000-0000-0000-000000000000", "c0000000-0000-0000-0000-000000000000"),
					makeRange("c0000000-0000-0000-0000-000000000000", "ffffffff-ffff-ffff-ffff-ffffffffffff"),
				},
				"eu": {
					makeRange("00000000-0000-0000-0000-000000000000", "40000000-0000-0000-0000-000000000000"),
					makeRange("40000000-0000-0000-0000-000000000000", "80000000-0000-0000-0000-000000000000"),
					makeRange("80000000-0000-0000-0000-000000000000", "c0000000-0000-0000-0000-000000000000"),
					makeRange("c0000000-0000-0000-0000-000000000000", "ffffffff-ffff-ffff-ffff-ffffffffffff"),
				},
				"asia": {
					makeRange("00000000-0000-0000-0000-000000000000", "40000000-0000-0000-0000-000000000000"),
					makeRange("40000000-0000-0000-0000-000000000000", "80000000-0000-0000-0000-000000000000"),
					makeRange("80000000-0000-0000-0000-000000000000", "c0000000-0000-0000-0000-000000000000"),
					makeRange("c0000000-0000-0000-0000-000000000000", "ffffffff-ffff-ffff-ffff-ffffffffffff"),
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			name := model.QualifiedDomainName{Service: service.Name(), Domain: "test-domain"}
			sp := model.NewShardingPolicy(tt.targetShards, model.WithShardingPolicyShards(tt.customShards))
			opts := model.WithDomainConfig(model.NewDomainConfig(model.WithDomainShardingPolicy(sp)))
			domain, err := model.NewDomain(name, model.Regional, time.Now(), opts)
			require.NoError(t, err)

			for region, expectedShards := range tt.regionResults {
				t.Run(string(region), func(t *testing.T) {
					actual := findShardsForRegion(domain, region)

					assertx.Equal(t, len(expectedShards), len(actual), "for region %v: expected %d shards, got %d", region, len(expectedShards), len(actual))
					assertSameShards(t, expectedShards, actual)
				})
			}
		})
	}
}

func assertSameShards(t *testing.T, expected, actual []uuidx.Range) {
	t.Helper()

	expectedShards := slicex.Map(expected, uuidx.Range.String)
	actualShards := slicex.Map(actual, uuidx.Range.String)

	assertx.Equal(t, expectedShards, actualShards, "shards don't match")
}

func makeRange(from, to string) uuidx.Range {
	return uuidx.MustNewRange(uuid.MustParse(from), uuid.MustParse(to))
}

func makeWork(regions ...model.Region) Work {
	return Work{
		Data: slicex.Map(regions, func(r model.Region) location.Location {
			return location.Location{Region: r}
		}),
	}
}
