package coordinator

import (
	"time"

	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pkg/util/p2quantile"
)

const (
	// P50 quantile to track median value
	median = 0.5
	// Score is ranged in [0, scoreRange)
	scoreRange = 100.0
)

// score represents the load score of a shard. It is in range (0, 100)
type score float64

// aggregatedLoad tracks aggregated load for a domain and its shards.
type aggregatedLoad struct {
	domainLoad model.Load
	shardLoad  map[model.Shard]model.Load
}

// load returns aggregated domain load
func (l *aggregatedLoad) load() model.Load {
	return l.domainLoad
}

// score returns score of the given shard if exists.
func (l *aggregatedLoad) score(shard model.Shard) (score, bool) {
	if l.domainLoad == 0 {
		return 0, false
	}

	ld, ok := l.shardLoad[shard]
	if !ok {
		return 0, false
	}

	// ld is median load of the given shard, l.domainLoad is the median load of the whole domain.
	// Following formula give the shard score in [0, scoreRange)
	return score(scoreRange * ld / (ld + l.domainLoad)), true
}

// quantileTracker tracks P2Quantiles for domain and its shards.
type quantileTracker struct {
	createdAt      time.Time
	domainQuantile *p2quantile.P2Quantile
	shardQuantile  map[model.Shard]*p2quantile.P2Quantile
}

// newTracker creates a new quantilTracker
func newTracker(now time.Time) *quantileTracker {
	tl, _ := p2quantile.New(median)

	return &quantileTracker{
		createdAt:      now,
		domainQuantile: tl,
		shardQuantile:  map[model.Shard]*p2quantile.P2Quantile{},
	}
}

// add adds an observation to quantile trackers.
func (t *quantileTracker) add(shard model.Shard, load model.Load) {
	t.domainQuantile.Add(float64(load))
	p, ok := t.shardQuantile[shard]
	if !ok {
		p, _ = p2quantile.New(median)
		t.shardQuantile[shard] = p
	}
	p.Add(float64(load))
}

// load return aggregatedLoad of current quantilTracker, called when the loadTracker is rotating.
func (t *quantileTracker) load() *aggregatedLoad {
	dl, _ := t.domainQuantile.Quantile()
	shardLoads := map[model.Shard]model.Load{}
	for shard, q := range t.shardQuantile {
		l, _ := q.Quantile()
		shardLoads[shard] = model.Load(l)
	}

	return &aggregatedLoad{
		domainLoad: model.Load(dl),
		shardLoad:  shardLoads,
	}
}

// needsRotation check if current quantileTracker needs to rotate.
func (t *quantileTracker) needsRotation(now time.Time, rotateInterval time.Duration) bool {
	return now.Sub(t.createdAt) > rotateInterval
}

// loadTracker manages a quantileTracker that accepts new observations and an aggregatedLoad that represents load from prev quantileTracker.
type loadTracker struct {
	rotateInterval time.Duration
	prevLoad       *aggregatedLoad
	currTracker    *quantileTracker
}

func newLoadTracker(now time.Time, rotateInterval time.Duration) *loadTracker {
	return &loadTracker{
		rotateInterval: rotateInterval,
		currTracker:    newTracker(now),
	}
}

// rotateIfNeeded seals current load tracker and creates a new one if needed.
func (l *loadTracker) rotateIfNeeded(now time.Time) {
	if l.currTracker.needsRotation(now, l.rotateInterval) {
		ld := l.currTracker.load()

		l.prevLoad = ld
		l.currTracker = newTracker(now)
	}
}

// add adds an observation of a shard load.
func (l *loadTracker) add(shard model.Shard, load model.Load) {
	l.currTracker.add(shard, load)
}

// domainLoad returns the load of a domain.
func (l *loadTracker) domainLoad() (model.Load, bool) {
	if l.prevLoad == nil {
		return 0, false
	}
	return l.prevLoad.load(), true
}

// shardLoad returns a map of <shard, Load>
func (l *loadTracker) shardLoad() (map[model.Shard]model.Load, bool) {
	if l.prevLoad == nil {
		return nil, false
	}
	return l.prevLoad.shardLoad, true
}

// shardScoreOrDefault returns score of a shard if it has been tracked.
// Or, default score that equals to (scoreRange / 2).
func (l *loadTracker) shardScoreOrDefault(shard model.Shard) score {
	defaultScore := score(scoreRange / 2)
	if l.prevLoad == nil {
		return defaultScore
	}

	s, ok := l.prevLoad.score(shard)
	if !ok {
		return defaultScore
	}
	return s
}
