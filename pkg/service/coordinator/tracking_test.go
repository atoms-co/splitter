package coordinator

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.atoms.co/splitter/pkg/model"
)

func testStart() time.Time {
	return time.Date(2025, 5, 20, 12, 0, 0, 0, time.UTC)
}

func testShard() model.Shard {
	return model.Shard{
		Domain: model.MustParseQualifiedDomainNameStr("tenant/service/domain"),
		Type:   model.Global,
		From:   model.MustParseKey("00000000-0000-0000-0000-000000000000"),
		To:     model.MustParseKey("80000000-0000-0000-0000-000000000000"),
	}
}

func TestQuantileTracker_FirstAdd(t *testing.T) {
	t.Parallel()

	start := testStart()
	qt := newTracker(start)
	shard := testShard()

	qt.add(shard, model.Load(42))

	agg := qt.load()
	require.Equal(t, model.Load(42), agg.load())
	require.Len(t, agg.shardLoad, 1)
	require.Equal(t, model.Load(42), agg.shardLoad[shard])
}

func TestQuantileTracker_NeedRotate(t *testing.T) {
	t.Parallel()

	start := testStart()
	qt := newTracker(start)
	rotateInterval := 10 * time.Minute

	require.False(t, qt.needsRotation(start, rotateInterval))
	require.False(t, qt.needsRotation(start.Add(rotateInterval), rotateInterval))
	require.True(t, qt.needsRotation(start.Add(rotateInterval+time.Nanosecond), rotateInterval))
}

func TestLoadTracker_TryRotatePublishesMetrics(t *testing.T) {
	t.Parallel()

	start := testStart()
	shard := testShard()

	t.Run("before rotate interval", func(t *testing.T) {
		rotateInterval := 10 * time.Minute
		tr := newLoadTracker(start, rotateInterval)
		for i := 0; i < 10; i++ {
			tr.add(shard, model.Load(10))
		}
		tr.rotateIfNeeded(start.Add(rotateInterval))

		_, ok := tr.domainLoad()
		require.False(t, ok)

		_, ok = tr.shardLoad()
		require.False(t, ok)
	})

	t.Run("after rotate interval", func(t *testing.T) {
		rotateInterval := 10 * time.Minute
		tr := newLoadTracker(start, rotateInterval)
		for i := 0; i < 10; i++ {
			tr.add(shard, model.Load(10))
		}
		tr.rotateIfNeeded(start.Add(rotateInterval + time.Second))

		load, ok := tr.domainLoad()
		require.True(t, ok)
		require.InDelta(t, float64(10), float64(load), 0.01)

		loads, ok := tr.shardLoad()
		require.True(t, ok)
		require.Len(t, loads, 1)
		require.InDelta(t, float64(10), float64(loads[shard]), 0.01)

		score := tr.shardScoreOrDefault(shard)
		require.True(t, ok)
		require.InDelta(t, float64(50), float64(score), 0.01)
	})
}

func TestLoadTracker_SecondRotate(t *testing.T) {
	t.Parallel()

	rotateInterval := 10 * time.Minute
	start := testStart()
	tr := newLoadTracker(start, rotateInterval)
	shard := testShard()

	for i := 0; i < 10; i++ {
		tr.add(shard, model.Load(20))
	}
	firstRotate := start.Add(rotateInterval + time.Second)
	tr.rotateIfNeeded(firstRotate)

	for i := 0; i < 10; i++ {
		tr.add(shard, model.Load(5))
	}
	tr.rotateIfNeeded(firstRotate.Add(rotateInterval + time.Second))

	load, ok := tr.domainLoad()
	require.True(t, ok)
	require.InDelta(t, float64(5), float64(load), 0.01)

	loads, ok := tr.shardLoad()
	require.True(t, ok)
	require.Len(t, loads, 1)
	require.InDelta(t, float64(5), float64(loads[shard]), 0.01)

	score := tr.shardScoreOrDefault(shard)
	require.InDelta(t, float64(50), float64(score), 0.01)
}

func TestLoadTracker_ShardScoreOrDefault(t *testing.T) {
	t.Parallel()

	rotateInterval := 10 * time.Minute
	start := testStart()
	tr := newLoadTracker(start, rotateInterval)
	shard := testShard()

	require.Equal(t, score(50), tr.shardScoreOrDefault(shard))

	tr.add(shard, model.Load(10))
	tr.rotateIfNeeded(start.Add(rotateInterval + time.Second))

	require.Equal(t, score(50), tr.shardScoreOrDefault(shard))
}
