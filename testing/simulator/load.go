package simulator

import (
	"fmt"
	"math"
	rand "math/rand/v2"

	"go.atoms.co/splitter/pkg/core"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pkg/util/p2quantile"
)

// loadForShard spaces scores across the inclusive range. A singleton uses the
// midpoint; when there are more than 99 shards, rounding repeats some scores.
func (strategy ShardLoadType) loadForShard(index, count int) int {
	if strategy == Fixed || count == 1 {
		return 50
	}
	return 1 + int(math.Round(98*float64(index)/float64(count-1)))
}

// shardLoadScores uses a separate setup stream so score sampling cannot affect
// region assignment or churn. Scores remain attached to synthetic shard indices.
func (c Config) shardLoadScores() []int {
	rng := rand.New(rand.NewPCG(uint64(c.Seed), 2))
	loads := make([]int, c.Shards)
	if c.ShardLoadType != CustomizedDistribution {
		for i, rank := range rng.Perm(c.Shards) {
			loads[i] = c.ShardLoadType.loadForShard(rank, c.Shards)
		}
		return loads
	}
	for i := range loads {
		loads[i] = sampleLoad(rng, c.ShardLoadDistribution)
	}
	// A separate stream for group overrides preserves all fallback shard scores.
	groupRNG := rand.New(rand.NewPCG(uint64(c.Seed), 3))
	regionOrder := c.shardRegionOrder()
	index := 0
	for _, group := range c.ShardRegionPreferences {
		if group.ShardLoadDistribution != nil {
			for _, shardIndex := range regionOrder[index : index+group.Shards] {
				loads[shardIndex] = sampleLoad(groupRNG, group.ShardLoadDistribution)
			}
		}
		index += group.Shards
	}
	return loads
}

func sampleLoad(rng *rand.Rand, percentages []float64) int {
	total := 0.0
	for _, percentage := range percentages {
		total += percentage
	}
	draw := rng.Float64() * total
	bucket := 0
	for ; bucket < len(percentages)-1; bucket++ {
		if draw < percentages[bucket] {
			break
		}
		draw -= percentages[bucket]
	}
	lower, upper := max(1, bucket*10), (bucket+1)*10
	return lower + rng.IntN(upper-lower)
}

func (c Config) shardRegionOrder() []int {
	return rand.New(rand.NewPCG(uint64(c.Seed), 1)).Perm(c.Shards)
}

func shardName(shard model.Shard) string {
	return fmt.Sprintf("%s@%s[%s,%s)", shard.Domain, shard.Region, shard.From, shard.To)
}

func (s *simulation) initializeShardLoads(domains []model.Domain) []core.ServiceStatus {
	// Randomize synthetic shard identities across native preference groups, then
	// assign global or preference-group scores to those identities. Separate
	// streams leave lifecycle scheduling untouched and preserve region assignments
	// when switching load types with the same seed.
	regionOrder := s.cfg.shardRegionOrder()
	loads := s.cfg.shardLoadScores()
	s.report.ShardLoads = make([]ShardLoad, s.cfg.Shards)
	s.shardLoads = make(map[model.Shard]int, s.cfg.Shards)
	s.shardPreferences = make(map[model.Shard][]string, s.cfg.Shards)
	domainLoads := make([]core.DomainLoadInfo, 0, len(domains))
	index := 0
	for _, domain := range domains {
		region := model.Region("")
		if domain.Type() == model.Regional {
			region = "centralus"
		}
		shards := model.NewShards(domain.Name(), domain.Type(), region, domain.Config().ShardingPolicy().Shards())
		preferences := make([]string, 0, len(domain.Regions()))
		for _, r := range domain.Regions() {
			preferences = append(preferences, string(r))
		}
		quantiles := make([]core.ShardQuantileInfo, 0, len(shards))
		for _, shard := range shards {
			shardIndex := regionOrder[index]
			index++
			load := loads[shardIndex]
			s.shardLoads[shard] = load
			s.shardPreferences[shard] = preferences
			s.report.ShardLoads[shardIndex] = ShardLoad{Index: shardIndex, Shard: shardName(shard), Load: load, PreferredRegions: preferences}
			// Invert the production quantile-to-integer score conversion at the middle
			// of the desired bucket, avoiding floating point rounding to load-1.
			const serviceQuantile = 100.0
			score := float64(load) + 0.5
			quantile := serviceQuantile * score / (100 - score)
			quantiles = append(quantiles, core.NewShardQuantileInfo(core.NewShard(shard.From, shard.To, shard.Region), quantile))
		}
		domainTracker, _ := p2quantile.New(0.5)
		published := core.NewDomainQuantileInfo(100, quantiles)
		domainLoads = append(domainLoads, core.NewDomainLoadInfo(domain.Name().Domain,
			core.NewDomainTrackerSnapshot(s.start, core.SnapshotQuantile(domainTracker), nil), &published))
	}
	if s.cfg.ShardLoadType == Fixed {
		return nil
	} // Default allocator load is 50.
	// Empty observation windows preserve the published static scores on rotation.
	serviceTracker, _ := p2quantile.New(0.75)
	load := core.NewServiceLoadInfo(s.service, domainLoads,
		core.WithServiceTrackerSnapshot(core.NewServiceTrackerSnapshot(s.start, core.SnapshotQuantile(serviceTracker))),
		core.WithServiceQuantileInfo(core.NewServiceQuantileInfo(100)))
	return []core.ServiceStatus{core.NewServiceStatus(load)}
}
