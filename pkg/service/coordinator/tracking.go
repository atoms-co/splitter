package coordinator

import (
	"time"

	"go.atoms.co/lib/mapx"
	"go.atoms.co/splitter/pkg/core"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pkg/util/p2quantile"
)

const (
	// P50 quantile to track median value
	median = 0.5
	// Score is ranged in [0, scoreRange)
	scoreRange = 100.0
	// defaultRotationInterval defines the interval a domainLoadTracker lives before rotation.
	defaultRotationInterval = 24 * time.Hour
)

// score represents the load score of a shard. It is in range (0, 100)
type score float64

// domainQuantileInfo holds published quantile values for a domain and its shards.
// Mutable version of core.DomainQuantileInfo.
type domainQuantileInfo struct {
	domainQuantile float64
	shardQuantiles map[core.Shard]float64
}

func (q *domainQuantileInfo) score(shard core.Shard) (score, bool) {
	sq, ok := q.shardQuantiles[shard]
	if !ok {
		return 0, false
	}

	// Should not happen as the load has been adjusted to at least 1 in domainTracker.add().
	// Checking for zero to avoid dividing by zero.
	if (q.domainQuantile + sq) == 0 {
		return 0, false
	}

	// sq is the median load for the shard.
	// The following formula gives the shard score in [0, scoreRange).
	return score(scoreRange * sq / (q.domainQuantile + sq)), true
}

// quantiles converts the current instance to core.DomainQuantileInfo.
func (q *domainQuantileInfo) quantiles() *core.DomainQuantileInfo {
	shardQuantiles := mapx.MapToSlice(q.shardQuantiles, core.NewShardQuantileInfo)
	ret := core.NewDomainQuantileInfo(q.domainQuantile, shardQuantiles)
	return &ret
}

// restoreDomainQuantileInfo restores quantiles from core.DomainQuantileInfo.
func restoreDomainQuantileInfo(q core.DomainQuantileInfo) (*domainQuantileInfo, error) {
	shardQuantiles := map[core.Shard]float64{}
	for _, sq := range q.ShardQuantiles() {
		shard, err := sq.Shard()
		if err != nil {
			return nil, err
		}
		shardQuantiles[shard] = sq.Quantile()
	}

	return &domainQuantileInfo{
		domainQuantile: q.DomainQuantile(),
		shardQuantiles: shardQuantiles,
	}, nil
}

// domainTracker tracks P2Quantiles for a domain and its shards.
// Mutable version of core.DomainTrackerSnapshot.
type domainTracker struct {
	createdAt time.Time

	domainQuantile *p2quantile.P2Quantile
	shardQuantiles map[core.Shard]*p2quantile.P2Quantile
}

func newDomainTracker(now time.Time) *domainTracker {
	tl, _ := p2quantile.New(median)

	return &domainTracker{
		createdAt:      now,
		domainQuantile: tl,
		shardQuantiles: map[core.Shard]*p2quantile.P2Quantile{},
	}
}

// add adds an observation.
func (t *domainTracker) add(shard model.Shard, load model.Load) {
	// Adjust load to at least 1.
	adjustedLoad := max(load, model.Load(1))

	t.domainQuantile.Add(float64(adjustedLoad))
	s := core.NewShard(shard.From, shard.To, shard.Region)
	p, ok := t.shardQuantiles[s]
	if !ok {
		p, _ = p2quantile.New(median)
		t.shardQuantiles[s] = p
	}
	p.Add(float64(adjustedLoad))
}

// publish publishes domainQuantileInfo from current domainTracker.
func (t *domainTracker) quantileInfo() (*domainQuantileInfo, bool) {
	dq, ok := t.domainQuantile.Quantile()
	if !ok {
		return nil, false
	}

	sq := mapx.MapIf(t.shardQuantiles, func(shard core.Shard, qt *p2quantile.P2Quantile) (core.Shard, float64, bool) {
		if q, ok := qt.Quantile(); ok {
			return shard, q, true
		}
		return shard, 0.0, false
	})

	return &domainQuantileInfo{
		domainQuantile: dq,
		shardQuantiles: sq,
	}, true
}

// needsRotation reports whether the current tracker instance should rotate.
func (t *domainTracker) needsRotation(now time.Time) bool {
	return now.Sub(t.createdAt) > defaultRotationInterval
}

func (t *domainTracker) domainLoad() (model.Load, bool) {
	dl, ok := t.domainQuantile.Quantile()
	if !ok {
		return 0, false
	}

	return model.Load(dl), true
}

func (t *domainTracker) shardLoad() map[core.Shard]model.Load {
	return mapx.MapIf(t.shardQuantiles, func(s core.Shard, q *p2quantile.P2Quantile) (core.Shard, model.Load, bool) {
		ld, ok := q.Quantile()
		return s, model.Load(ld), ok
	})
}

// snapshot takes a snapshot from current domainTracker.
func (t *domainTracker) snapshot() core.DomainTrackerSnapshot {
	shardTrackers := mapx.MapToSlice(t.shardQuantiles, func(s core.Shard, q *p2quantile.P2Quantile) core.ShardP2QuantileSnapshot {
		return core.NewShardP2QuantileSnapshot(s, core.SnapshotQuantile(q))
	})

	return core.NewDomainTrackerSnapshot(t.createdAt, core.SnapshotQuantile(t.domainQuantile), shardTrackers)
}

// restoreDomainTracker restores a domainTracker instance from core.DomainTrackerSnapshot
func restoreDomainTracker(t core.DomainTrackerSnapshot) (*domainTracker, error) {
	domainQuantile, err := t.DomainSnapshot().Restore()
	if err != nil {
		return nil, err
	}

	shardTrackers := map[core.Shard]*p2quantile.P2Quantile{}
	for _, ss := range t.ShardSnapshot() {
		shard, err := ss.Shard()
		if err != nil {
			return nil, err
		}

		q, err := ss.Snapshot().Restore()
		if err != nil {
			return nil, err
		}

		shardTrackers[shard] = q
	}

	return &domainTracker{
		createdAt:      t.CreatedAt(),
		domainQuantile: domainQuantile,
		shardQuantiles: shardTrackers,
	}, nil
}

// domainLoadTracker manages active domainTracker and published quantiles for a domain and its shards.
// Mutable version of core.DomainLoadTracker.
type domainLoadTracker struct {
	domain   model.DomainName
	quantile *domainQuantileInfo
	tracker  *domainTracker
}

func newDomainLoadTracker(now time.Time, domain model.DomainName) *domainLoadTracker {
	return &domainLoadTracker{
		domain:  domain,
		tracker: newDomainTracker(now),
	}
}

// rotateIfNeeded seals current load tracker and creates a new one if needed.
func (t *domainLoadTracker) rotateIfNeeded(now time.Time) {
	if t.tracker.needsRotation(now) {
		if q, ok := t.tracker.quantileInfo(); ok {
			t.quantile = q
		}
		t.tracker = newDomainTracker(now)
	}
}

// add adds an observation of a shard load.
func (t *domainLoadTracker) add(shard model.Shard, load model.Load) {
	t.tracker.add(shard, load)
}

// domainLoad returns domain load from active tracker.
func (t *domainLoadTracker) domainLoad() (model.Load, bool) {
	return t.tracker.domainLoad()
}

// shardLoad returns shard load from the active tracker.
func (t *domainLoadTracker) shardLoad() map[core.Shard]model.Load {
	return t.tracker.shardLoad()
}

// shardScoreOrDefault returns score of a shard if it has been tracked.
// Or, default score that equals to (scoreRange / 2).
func (t *domainLoadTracker) shardScoreOrDefault(shard core.Shard) score {
	defaultScore := score(scoreRange / 2)
	if t.quantile == nil {
		return defaultScore
	}

	s, ok := t.quantile.score(shard)
	if !ok {
		return defaultScore
	}
	return s
}

// snapshot takes a snapshot of the current domainLoadTracker instance to a core.DomainLoadTracker.
func (t *domainLoadTracker) snapshot() core.DomainLoadInfo {
	var q *core.DomainQuantileInfo
	if t.quantile != nil {
		q = t.quantile.quantiles()
	}

	return core.NewDomainLoadInfo(t.domain, t.tracker.snapshot(), q)
}

// restoreDomainLoadTracker restores a domainLoadTracker from core.DomainLoadTracker.
func restoreDomainLoadTracker(t core.DomainLoadInfo) (*domainLoadTracker, error) {
	var err error
	var restored *domainQuantileInfo
	if t.HasQuantileInfo() {
		restored, err = restoreDomainQuantileInfo(t.QuantileInfo())
		if err != nil {
			return nil, err
		}
	}

	rt, err := restoreDomainTracker(t.TrackerSnapshot())
	if err != nil {
		return nil, err
	}

	return &domainLoadTracker{
		domain:   t.DomainName(),
		quantile: restored,
		tracker:  rt,
	}, nil
}
