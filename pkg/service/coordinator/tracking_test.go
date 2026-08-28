package coordinator

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.atoms.co/splitter/pkg/core"
	"go.atoms.co/splitter/pkg/model"
)

const epsilon = 1e-6

func testStart() time.Time {
	return time.Date(2025, 5, 20, 12, 0, 0, 0, time.UTC)
}

func testShard() model.Shard {
	domain := model.MustParseQualifiedDomainNameStr("tenant/service/domain")
	from := model.MustParseKey("00000000-0000-0000-0000-000000000000")
	to := model.MustParseKey("80000000-0000-0000-0000-000000000000")
	return newShard(domain, from, to)
}

func newShard(domain model.QualifiedDomainName, from model.Key, to model.Key) model.Shard {
	return model.Shard{
		Domain: domain,
		Type:   model.Global,
		From:   from,
		To:     to,
	}
}

func TestQuantileTracker_FirstAdd(t *testing.T) {
	t.Parallel()

	start := testStart()
	qt := newDomainTracker(start)
	shard := testShard()

	qt.add(shard, model.Load(42))

	dl, ok := qt.domainLoad()
	require.True(t, ok)
	require.Equal(t, model.Load(42), dl)

	sl := qt.shardLoad()
	require.Len(t, sl, 1)
	sps := core.NewShard(shard.From, shard.To, shard.Region)
	require.Equal(t, model.Load(42), sl[sps])
}

func TestDomainLoadTracker_RotatePublishesMetrics(t *testing.T) {
	t.Parallel()

	start := testStart()
	shard := testShard()

	t.Run("before rotation", func(t *testing.T) {
		tr := newDomainLoadTracker(start, "domain")
		for range 10 {
			tr.add(shard, model.Load(10))
		}
		require.Nil(t, tr.quantile)
		require.False(t, tr.snapshot().HasQuantileInfo())
	})

	t.Run("after rotation", func(t *testing.T) {
		tr := newDomainLoadTracker(start, "domain")
		for range 10 {
			tr.add(shard, model.Load(10))
		}
		require.Nil(t, tr.quantile)
		require.True(t, tr.rotate(start.Add(defaultRotationInterval+time.Second)))

		require.NotNil(t, tr.quantile)
		require.True(t, tr.snapshot().HasQuantileInfo())

		_, ok := tr.domainLoad()
		require.False(t, ok, "active tracker is reset after rotation")
		require.Empty(t, tr.shardLoad())
	})
}

func TestQuantileTracker_MultiShardLoadAndScore(t *testing.T) {
	t.Parallel()

	domain := model.MustParseQualifiedDomainNameStr("tenant/service/domain")
	shard1 := newShard(domain, model.MustParseKey("00000000-0000-0000-0000-000000000000"), model.MustParseKey("7fffffff-ffff-ffff-ffff-ffffffffffff"))
	shard2 := newShard(domain, model.MustParseKey("80000000-0000-0000-0000-000000000000"), model.MustParseKey("ffffffff-ffff-ffff-ffff-ffffffffffff"))

	start := testStart()
	qt := newDomainTracker(start)
	key1 := core.NewShard(shard1.From, shard1.To, shard1.Region)
	key2 := core.NewShard(shard2.From, shard2.To, shard2.Region)

	const n = 10
	for range n {
		qt.add(shard1, model.Load(10))
	}
	for range n {
		qt.add(shard2, model.Load(30))
	}

	dl, ok := qt.domainLoad()
	require.True(t, ok)
	require.Greater(t, float64(dl), float64(10))
	require.Less(t, float64(dl), float64(30))

	sl := qt.shardLoad()
	require.Len(t, sl, 2)
	load1 := sl[key1]
	load2 := sl[key2]
	require.Equal(t, model.Load(10), load1)
	require.Equal(t, model.Load(30), load2)

	published, ok := qt.quantileInfo()
	require.True(t, ok)

	sq1, ok := qt.shardQuantiles[key1].Quantile()
	require.True(t, ok)
	sq2, ok := qt.shardQuantiles[key2].Quantile()
	require.True(t, ok)

	serviceQuantile := float64(20)
	score1, ok := published.score(key1, serviceQuantile)
	require.True(t, ok)
	require.InDelta(t, float64(scoreFromQuantiles(serviceQuantile, sq1)), float64(score1), epsilon)

	score2, ok := published.score(key2, serviceQuantile)
	require.True(t, ok)
	require.InDelta(t, float64(scoreFromQuantiles(serviceQuantile, sq2)), float64(score2), epsilon)
	require.Greater(t, score2, score1)
	require.Less(t, score1, score(scoreRange/2))
	require.Greater(t, score2, score(scoreRange/2))
}

func TestDomainQuantileInfo_ShardScore(t *testing.T) {
	t.Parallel()

	shardA := core.NewShard(model.MustParseKey("00000000-0000-0000-0000-000000000000"), model.MustParseKey("80000000-0000-0000-0000-000000000000"), "")
	shardB := core.NewShard(model.MustParseKey("80000000-0000-0000-0000-000000000000"), model.MustParseKey("ffffffff-ffff-ffff-ffff-ffffffffffff"), "")
	quantiles := &domainQuantileInfo{
		domainQuantile: 100,
		shardQuantiles: map[core.Shard]float64{
			shardA: 100,
			shardB: 10_000,
		},
	}
	serviceQuantile := float64(100)

	scoreA, ok := quantiles.score(shardA, serviceQuantile)
	require.True(t, ok)
	scoreB, ok := quantiles.score(shardB, serviceQuantile)
	require.True(t, ok)

	require.InDelta(t, 50, float64(scoreA), epsilon)
	require.InDelta(t, 100*10_000.0/10_100.0, float64(scoreB), epsilon)
	require.Greater(t, scoreB, scoreA)
}

func TestServiceLoadTracker_MessageRoundTrip(t *testing.T) {
	t.Parallel()

	start := testStart()
	original := newServiceLoadTracker(start)
	for range 10 {
		original.add(model.Load(20))
	}
	require.True(t, original.rotate(start.Add(defaultRotationInterval+time.Second)))
	for range 5 {
		original.add(model.Load(8))
	}

	snapshot, quantile := original.snapshot()
	serviceLoad := core.NewServiceLoadInfo(
		model.MustParseQualifiedServiceNameStr("tenant/service"),
		nil,
		core.WithServiceTrackerSnapshot(snapshot),
		core.WithServiceQuantileInfo(*quantile),
	)
	restored, err := restoreServiceLoadTracker(start, serviceLoad)
	require.NoError(t, err)
	require.NotNil(t, restored.quantile)
	require.InDelta(t, 20, *restored.quantile, epsilon)
	load, ok := restored.serviceLoad()
	require.True(t, ok)
	require.InDelta(t, 8, float64(load), epsilon)
}

func TestDomainLoadTracker_MessageRoundTrip(t *testing.T) {
	t.Parallel()

	start := testStart()
	shard := testShard()

	original := newDomainLoadTracker(start, "domain")
	for range 10 {
		original.add(shard, model.Load(20))
	}
	original.rotate(start.Add(defaultRotationInterval + time.Second))
	for range 5 {
		original.add(shard, model.Load(8))
	}

	restored, err := restoreDomainLoadTracker(original.snapshot())
	require.NoError(t, err)

	require.True(t, restored.snapshot().HasQuantileInfo())
	require.Equal(t, original.quantile, restored.quantile)

	load, ok := restored.domainLoad()
	require.True(t, ok)
	require.InDelta(t, float64(8), float64(load), 0.01)
}

func TestLoadTracker_OwnsServiceAndDomainTrackers(t *testing.T) {
	t.Parallel()

	start := testStart()
	tracker := newLoadTracker(start)
	globalShard := testShard()
	unitShard := model.Shard{
		Domain: model.MustParseQualifiedDomainNameStr("tenant/service/unit"),
		Type:   model.Unit,
	}

	for range 10 {
		tracker.add(start, globalShard, model.Load(100))
		tracker.add(start, unitShard, model.Load(1_000))
	}

	require.Len(t, tracker.domains, 2)
	serviceLoad, ok := tracker.service.serviceLoad()
	require.True(t, ok)
	require.Equal(t, model.Load(400), serviceLoad, "service load must include observations from every domain type")

	require.True(t, tracker.rotateIfNeeded(start.Add(defaultRotationInterval+time.Second)))
	require.False(t, tracker.rotateIfNeeded(start.Add(defaultRotationInterval+2*time.Second)))

	snapshot := tracker.snapshot(globalShard.Domain.Service)
	require.True(t, snapshot.HasTrackerSnapshot())
	require.True(t, snapshot.HasQuantileInfo())
	require.Len(t, snapshot.Domains(), 2)
	for _, domain := range tracker.domains {
		_, ok := domain.domainLoad()
		require.False(t, ok, "active domain trackers must reset together")
	}
}

func TestLoadTracker_RotationOnlyPublishesMatureDomains(t *testing.T) {
	t.Parallel()

	start := testStart()
	now := start.Add(defaultRotationInterval + time.Second)
	eligibleDomainStart := now.Add(-domainPublicationAge)
	newDomainStart := eligibleDomainStart.Add(time.Nanosecond)
	tracker := newLoadTracker(start)
	matureShard := testShard()
	eligibleShard := model.Shard{
		Domain: model.MustParseQualifiedDomainNameStr("tenant/service/eligible-domain"),
		Type:   model.Unit,
	}
	newShard := model.Shard{
		Domain: model.MustParseQualifiedDomainNameStr("tenant/service/new-domain"),
		Type:   model.Unit,
	}

	for range 10 {
		tracker.add(start, matureShard, model.Load(20))
		tracker.add(eligibleDomainStart, eligibleShard, model.Load(30))
		tracker.add(newDomainStart, newShard, model.Load(40))
	}

	require.True(t, tracker.rotateIfNeeded(now))
	require.NotNil(t, tracker.service.quantile)
	require.NotNil(t, tracker.domains[matureShard.Domain].quantile)
	require.NotNil(t, tracker.domains[eligibleShard.Domain].quantile, "a domain tracker at the publication interval must publish quantiles")
	require.Nil(t, tracker.domains[newShard.Domain].quantile, "an immature domain tracker must not publish quantiles")
	require.Equal(t, defaultShardScore, tracker.shardScore(newShard.Domain, core.NewShard(newShard.From, newShard.To, newShard.Region)))

	for _, domain := range tracker.domains {
		require.Equal(t, now, domain.tracker.createdAt)
		_, ok := domain.domainLoad()
		require.False(t, ok, "active domain trackers must align with the new service tracker")
	}
}

func TestLoadTracker_ResetActive(t *testing.T) {
	t.Parallel()

	start := testStart()
	tracker := newLoadTracker(start)
	shard := testShard()
	for range 10 {
		tracker.add(start, shard, model.Load(100))
	}
	require.True(t, tracker.rotateIfNeeded(start.Add(defaultRotationInterval+time.Second)))

	domainTracker := tracker.domains[shard.Domain]
	publishedDomainQuantile := domainTracker.quantile
	require.NotNil(t, publishedDomainQuantile)
	require.NotNil(t, tracker.service.quantile)

	for range 5 {
		tracker.add(start, shard, model.Load(200))
	}

	resetAt := start.Add(2 * defaultRotationInterval)
	tracker.resetActive(resetAt)

	require.Nil(t, tracker.service.quantile)
	require.Equal(t, resetAt, tracker.service.tracker.createdAt)
	_, ok := tracker.service.serviceLoad()
	require.False(t, ok)

	require.Same(t, publishedDomainQuantile, domainTracker.quantile)
	require.Equal(t, resetAt, domainTracker.tracker.createdAt)
	_, ok = domainTracker.domainLoad()
	require.False(t, ok)
}

func TestLoadTracker_ShardScore(t *testing.T) {
	t.Parallel()

	shard := testShard()
	shardSnapshot := core.NewShard(shard.From, shard.To, shard.Region)
	tracker := newLoadTracker(testStart())
	require.Equal(t, defaultShardScore, tracker.shardScore(shard.Domain, shardSnapshot))

	tracker.domains[shard.Domain] = &domainLoadTracker{
		domain: shard.Domain.Domain,
		quantile: &domainQuantileInfo{
			domainQuantile: 10_000,
			shardQuantiles: map[core.Shard]float64{shardSnapshot: 10_000},
		},
		tracker: newDomainTracker(testStart()),
	}
	serviceQuantile := float64(100)
	tracker.service.quantile = &serviceQuantile

	require.InDelta(t, 100*10_000.0/10_100.0, float64(tracker.shardScore(shard.Domain, shardSnapshot)), epsilon)

	tracker.service.quantile = nil
	require.Equal(t, defaultShardScore, tracker.shardScore(shard.Domain, shardSnapshot))
}

func scoreFromQuantiles(serviceQuantile, shardQuantile float64) score {
	if serviceQuantile+shardQuantile == 0 {
		return 0
	}
	return score(scoreRange * shardQuantile / (serviceQuantile + shardQuantile))
}
