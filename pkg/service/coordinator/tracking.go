package coordinator

import (
	"time"

	"go.atoms.co/lib/mapx"

	"go.atoms.co/splitter/pkg/core"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pkg/util/p2quantile"
)

// score represents the load score of a shard. It is in range (0, 100)
type score float64

const (
	// P50 quantile to track median value
	median = 0.5
	// Score is ranged in [0, scoreRange)
	scoreRange = 100.0
	// defaultShardScore is used until a shard has a published score.
	// It is also used as defaultShardLoad by services that doesn't enable load tracking.
	defaultShardScore score = scoreRange * median
	// defaultRotationInterval defines how long the service tracker lives before rotation.
	defaultRotationInterval = 24 * time.Hour
	// domainPublicationAge defines the minimum age of a domain tracker whose quantiles can be published.
	// serviceLoadTracker and domainLoadTracker tracks their own created_at, serviceLoadTracker is rotated (created) at fixed interval (defaultRotationInterval),
	// domainLoadTracker is created only when the coordinator receives load report from consumer, thus its created_at is different from serviceLoadTracker.
	// On one hand, when serviceLoadTracker is being rotated, most age of domainLoadTracker is less than defaultRotationInterval, so we can't use defaultRotationInterval
	// to decide if the domainLoadTracker could be published.
	// On other hand, some new domain might join in the middle of defaultRotationInterval and does not have enough data to publish its quantile.
	// Thus, domainPublicationAge is defined to determine if domainLoadTracker ages long enough to publish its quantile.
	domainPublicationAge = 20 * time.Hour
)

// domainQuantileInfo holds published quantile values for a domain and its shards.
// Mutable version of core.DomainQuantileInfo.
type domainQuantileInfo struct {
	domainQuantile float64
	shardQuantiles map[core.Shard]float64
}

func (q *domainQuantileInfo) score(shard core.Shard, serviceQuantile float64) (score, bool) {
	sq, ok := q.shardQuantiles[shard]
	if !ok {
		return 0, false
	}

	// Should not happen as the load has been adjusted to at least 1 in domainTracker.add().
	// Checking for zero to avoid dividing by zero.
	if (serviceQuantile + sq) == 0 {
		return 0, false
	}

	// sq is the median load for the shard.
	// The following formula gives the shard score in [0, scoreRange).
	return score(scoreRange * sq / (serviceQuantile + sq)), true
}

// serviceTracker tracks the P2Quantile for all shards in a service.
type serviceTracker struct {
	createdAt time.Time
	quantile  *p2quantile.P2Quantile
}

func newServiceTracker(now time.Time) *serviceTracker {
	q, _ := p2quantile.New(median)
	return &serviceTracker{createdAt: now, quantile: q}
}

func (t *serviceTracker) add(load model.Load) {
	t.quantile.Add(float64(max(load, model.Load(1))))
}

func (t *serviceTracker) needsRotation(now time.Time) bool {
	return now.Sub(t.createdAt) > defaultRotationInterval
}

func (t *serviceTracker) load() (model.Load, bool) {
	q, ok := t.quantile.Quantile()
	return model.Load(q), ok
}

func (t *serviceTracker) snapshot() core.ServiceTrackerSnapshot {
	return core.NewServiceTrackerSnapshot(t.createdAt, core.SnapshotQuantile(t.quantile))
}

func restoreServiceTracker(snapshot core.ServiceTrackerSnapshot) (*serviceTracker, error) {
	q, err := snapshot.Snapshot().Restore()
	if err != nil {
		return nil, err
	}
	return &serviceTracker{createdAt: snapshot.CreatedAt(), quantile: q}, nil
}

// serviceLoadTracker manages the active service tracker and the published service quantile.
type serviceLoadTracker struct {
	quantile *float64
	tracker  *serviceTracker
}

func newServiceLoadTracker(now time.Time) *serviceLoadTracker {
	return &serviceLoadTracker{tracker: newServiceTracker(now)}
}

func (t *serviceLoadTracker) add(load model.Load) {
	t.tracker.add(load)
}

func (t *serviceLoadTracker) needsRotation(now time.Time) bool {
	return t.tracker.needsRotation(now)
}

func (t *serviceLoadTracker) rotate(now time.Time) bool {
	updated := false
	if q, ok := t.tracker.quantile.Quantile(); ok {
		t.quantile = &q
		updated = true
	}
	t.tracker = newServiceTracker(now)
	return updated
}

func (t *serviceLoadTracker) serviceLoad() (model.Load, bool) {
	return t.tracker.load()
}

func (t *serviceLoadTracker) snapshot() (core.ServiceTrackerSnapshot, *core.ServiceQuantileInfo) {
	snapshot := t.tracker.snapshot()
	if t.quantile == nil {
		return snapshot, nil
	}
	quantile := core.NewServiceQuantileInfo(*t.quantile)
	return snapshot, &quantile
}

func restoreServiceLoadTracker(now time.Time, info core.ServiceLoadInfo) (*serviceLoadTracker, error) {
	if !info.HasTrackerSnapshot() {
		return newServiceLoadTracker(now), nil
	}

	tracker, err := restoreServiceTracker(info.TrackerSnapshot())
	if err != nil {
		return nil, err
	}
	ret := &serviceLoadTracker{tracker: tracker}
	if info.HasQuantileInfo() {
		q := info.QuantileInfo().Quantile()
		ret.quantile = &q
	}
	return ret, nil
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

// readyToPublish reports whether the current tracker is old enough to publish.
func (t *domainTracker) readyToPublish(now time.Time) bool {
	return now.Sub(t.createdAt) >= domainPublicationAge
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

// rotate seals the active tracker and starts a new one on the shared service rotation boundary.
// It reports whether new quantiles were published.
func (t *domainLoadTracker) rotate(now time.Time) bool {
	updated := false
	if q, ok := t.tracker.quantileInfo(); ok {
		t.quantile = q
		updated = true
	}
	t.tracker = newDomainTracker(now)
	return updated
}

// rotateWithService starts a new tracker aligned with the service tracker. The
// current quantiles are only published if the domain tracker has lived for the
// minimum publication interval.
func (t *domainLoadTracker) rotateWithService(now time.Time) bool {
	if t.tracker.readyToPublish(now) {
		return t.rotate(now)
	}
	t.tracker = newDomainTracker(now)
	return false
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

// loadTracker owns trackers for a service and each of its domains.
type loadTracker struct {
	service *serviceLoadTracker
	domains map[model.QualifiedDomainName]*domainLoadTracker
}

func newLoadTracker(now time.Time) *loadTracker {
	return &loadTracker{
		service: newServiceLoadTracker(now),
		domains: map[model.QualifiedDomainName]*domainLoadTracker{},
	}
}

func (t *loadTracker) add(now time.Time, shard model.Shard, load model.Load) {
	tracker, ok := t.domains[shard.Domain]
	if !ok {
		tracker = newDomainLoadTracker(now, shard.Domain.Domain)
		t.domains[shard.Domain] = tracker
	}
	tracker.add(shard, load)
	t.service.add(load)
}

func (t *loadTracker) rotateIfNeeded(now time.Time) bool {
	if !t.service.needsRotation(now) {
		return false
	}

	updated := t.service.rotate(now)
	for _, tracker := range t.domains {
		updated = tracker.rotateWithService(now) || updated
	}
	return updated
}

// resetActive starts a new observation window while preserving published domain quantiles as fallback scores.
func (t *loadTracker) resetActive(now time.Time) {
	t.service = newServiceLoadTracker(now)
	for _, tracker := range t.domains {
		tracker.tracker = newDomainTracker(now)
	}
}

func (t *loadTracker) shardScore(domain model.QualifiedDomainName, shard core.Shard) score {
	tracker, ok := t.domains[domain]
	if !ok || tracker.quantile == nil || t.service.quantile == nil {
		return defaultShardScore
	}

	if s, ok := tracker.quantile.score(shard, *t.service.quantile); ok {
		return s
	}
	return defaultShardScore
}

func (t *loadTracker) snapshot(service model.QualifiedServiceName) core.ServiceLoadInfo {
	domains := mapx.MapValues(t.domains, func(v *domainLoadTracker) core.DomainLoadInfo {
		return v.snapshot()
	})
	trackerSnapshot, quantileInfo := t.service.snapshot()
	opts := []core.ServiceLoadInfoOption{core.WithServiceTrackerSnapshot(trackerSnapshot)}
	if quantileInfo != nil {
		opts = append(opts, core.WithServiceQuantileInfo(*quantileInfo))
	}
	return core.NewServiceLoadInfo(service, domains, opts...)
}
