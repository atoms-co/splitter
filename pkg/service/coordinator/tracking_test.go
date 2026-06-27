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

func TestQuantileTracker_NeedRotate(t *testing.T) {
	t.Parallel()

	start := testStart()
	qt := newDomainTracker(start)

	require.False(t, qt.needsRotation(start))
	require.False(t, qt.needsRotation(start.Add(defaultRotationInterval)))
	require.True(t, qt.needsRotation(start.Add(defaultRotationInterval+time.Nanosecond)))
}

func TestLoadTracker_TryRotatePublishesMetrics(t *testing.T) {
	t.Parallel()

	start := testStart()
	shard := testShard()

	t.Run("before rotate interval", func(t *testing.T) {
		tr := newDomainLoadTracker(start, "domain")
		for i := 0; i < 10; i++ {
			tr.add(shard, model.Load(10))
		}
		require.Nil(t, tr.quantile)
		require.False(t, tr.snapshot().HasQuantileInfo())
	})

	t.Run("after rotate interval", func(t *testing.T) {
		tr := newDomainLoadTracker(start, "domain")
		for i := 0; i < 10; i++ {
			tr.add(shard, model.Load(10))
		}
		require.Nil(t, tr.quantile)
		tr.rotateIfNeeded(start.Add(defaultRotationInterval + time.Second))

		require.NotNil(t, tr.quantile)
		require.True(t, tr.snapshot().HasQuantileInfo())

		_, ok := tr.domainLoad()
		require.False(t, ok, "active tracker is reset after rotation")
		require.Empty(t, tr.shardLoad())

		sps := core.NewShard(shard.From, shard.To, shard.Region)
		score := tr.shardScoreOrDefault(sps)
		require.InDelta(t, float64(50), float64(score), epsilon)
	})
}

func TestLoadTracker_ShardScoreOrDefault(t *testing.T) {
	t.Parallel()

	start := testStart()
	tr := newDomainLoadTracker(start, "domain")
	shard := testShard()
	sps := core.NewShard(shard.From, shard.To, shard.Region)

	require.Equal(t, score(50), tr.shardScoreOrDefault(sps))

	tr.add(shard, model.Load(10))
	tr.rotateIfNeeded(start.Add(defaultRotationInterval + time.Second))

	require.Equal(t, score(50), tr.shardScoreOrDefault(sps))
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
	for i := 0; i < n; i++ {
		qt.add(shard1, model.Load(10))
	}
	for i := 0; i < n; i++ {
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

	dq, ok := qt.domainQuantile.Quantile()
	require.True(t, ok)
	sq1, ok := qt.shardQuantiles[key1].Quantile()
	require.True(t, ok)
	sq2, ok := qt.shardQuantiles[key2].Quantile()
	require.True(t, ok)

	score1, ok := published.score(key1)
	require.True(t, ok)
	require.InDelta(t, float64(scoreFromQuantiles(dq, sq1)), float64(score1), epsilon)

	score2, ok := published.score(key2)
	require.True(t, ok)
	require.InDelta(t, float64(scoreFromQuantiles(dq, sq2)), float64(score2), epsilon)
	require.Greater(t, score2, score1)
	require.Less(t, score1, score(scoreRange/2))
	require.Greater(t, score2, score(scoreRange/2))
}

func TestLoadTracker_MessageRoundTrip(t *testing.T) {
	t.Parallel()

	start := testStart()
	shard := testShard()

	original := newDomainLoadTracker(start, "domain")
	for i := 0; i < 10; i++ {
		original.add(shard, model.Load(20))
	}
	original.rotateIfNeeded(start.Add(defaultRotationInterval + time.Second))
	for i := 0; i < 5; i++ {
		original.add(shard, model.Load(8))
	}

	restored, err := restoreDomainLoadTracker(original.snapshot())
	require.NoError(t, err)

	require.True(t, restored.snapshot().HasQuantileInfo())
	sps := core.NewShard(shard.From, shard.To, shard.Region)
	require.Equal(t, score(50), restored.shardScoreOrDefault(sps))

	load, ok := restored.domainLoad()
	require.True(t, ok)
	require.InDelta(t, float64(8), float64(load), 0.01)
}

func scoreFromQuantiles(domainQuantile, shardQuantile float64) score {
	if domainQuantile+shardQuantile == 0 {
		return 0
	}
	return score(scoreRange * shardQuantile / (domainQuantile + shardQuantile))
}
