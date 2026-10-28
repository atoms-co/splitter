package simulator

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"
)

// Duration is a JSON duration string, such as "5s" or "1h".
type Duration time.Duration

func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

func (d *Duration) UnmarshalJSON(data []byte) error {
	var value string
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return fmt.Errorf("parse duration %q: %w", value, err)
	}
	*d = Duration(parsed)
	return nil
}

// ShardLoadType selects the static integer load score used by the allocator.
type ShardLoadType string

const (
	Fixed                  ShardLoadType = "Fixed"
	EvenDistribution       ShardLoadType = "EvenDistribution"
	CustomizedDistribution ShardLoadType = "CustomizedDistribution"
)

// ConsumerRegion assigns consecutive consumer slots to one region.
type ConsumerRegion struct {
	Region    string `json:"region"`
	Consumers int    `json:"consumers"`
	// DrainRate optionally overrides Config.DrainRate for each consumer slot in
	// this region, in attempts per minute. Nil inherits; explicit zero disables.
	// Reconnecting consumers and quit replacements retain the slot's rate.
	DrainRate *float64 `json:"drain_rate,omitempty"`
}

// ShardRegionPreference gives a group of shards equally preferred regions.
// An empty Regions list permits any region. Counts sum to Config.Shards.
type ShardRegionPreference struct {
	Shards  int      `json:"shards"`
	Regions []string `json:"regions"`
	// ShardLoadDistribution overrides Config.ShardLoadDistribution for this group
	// with CustomizedDistribution. Omitted uses the global distribution. Scores
	// belong to shards, regardless of which region subsequently serves them.
	ShardLoadDistribution []float64 `json:"shard_load_distribution,omitempty"`
}

// Config describes a single service with configurable consumer and shard regions.
// Rates are attempts per consumer slot per minute during the churn phase;
// attempts during a disconnect are recorded as skipped.
type Config struct {
	// ConsumerRegions optionally places initial consumer slots into regions; counts
	// must sum to Consumers. Omitted groups place every consumer in centralus.
	ConsumerRegions []ConsumerRegion `json:"consumer_regions,omitempty"`
	// ShardRegionPreferences optionally groups shards by equally preferred regions;
	// counts must sum to Shards. Omitted groups prefer centralus; an empty region
	// list within a group permits any region.
	ShardRegionPreferences []ShardRegionPreference `json:"shard_region_preferences,omitempty"`
	// ShardLoadType selects static allocator scores: Fixed uses 50 with tracking
	// disabled; EvenDistribution and CustomizedDistribution restore tracked scores.
	ShardLoadType ShardLoadType `json:"shard_load_type"`
	// ShardLoadDistribution supplies the default ten bucket percentages totaling
	// 100, required for CustomizedDistribution even when groups override it.
	// Buckets are [1,10), [10,20), ..., [90,100).
	ShardLoadDistribution []float64 `json:"shard_load_distribution,omitempty"`
	// Consumers is the positive population. Quit replacements reuse the same slots.
	Consumers int `json:"consumers"`
	// Shards is the positive total workload, including all region-preference groups.
	Shards int `json:"shards"`
	// Seed controls separate shard-score and region-assignment streams, plus
	// independent lifecycle streams per consumer slot and event type. Shard selection
	// has its own stream. Coordinator jitter and equal-priority choices remain unseeded.
	Seed int64 `json:"seed"`
	// Warmup allows initial assignment and balancing before churn starts. Its moves
	// are reported separately; zero starts churn immediately.
	Warmup Duration `json:"warmup"`
	// Duration is the positive window in which random lifecycle attempts may start.
	// Total virtual runtime is Warmup + Duration; unfinished work remains pending.
	Duration Duration `json:"duration"`
	// RelinquishRate is the Poisson attempt rate to immediately release one random
	// serving shard per consumer slot per minute. Zero disables random relinquishes.
	RelinquishRate float64 `json:"relinquish_rate"`
	// DrainRate is the Poisson attempt rate to request a coordinated handoff of one
	// active shard per consumer slot per minute. ConsumerRegions may override it.
	// Zero disables random drains for regions without an override.
	DrainRate float64 `json:"drain_rate"`
	// ReconnectRate is the Poisson transport-disconnect attempt rate per consumer
	// slot per minute. Zero disables random reconnects.
	ReconnectRate float64 `json:"reconnect_rate"`
	// ReconnectDelay is transport downtime before the same identity reconnects with
	// unexpired grants. Processing continues while disconnected until lease expiry.
	ReconnectDelay Duration `json:"reconnect_delay"`
	// QuitRate is the Poisson abrupt-stop attempt rate per consumer slot per minute.
	// A quit stops all its shards and schedules a fresh-identity replacement.
	// Zero disables random quits.
	QuitRate float64 `json:"quit_rate"`
}

func DefaultConfig() Config {
	return Config{ShardLoadType: Fixed, Consumers: 16, Shards: 128, Seed: 42,
		Warmup: Duration(5 * time.Minute), Duration: Duration(time.Hour),
		ReconnectDelay: Duration(5 * time.Second)}
}

func (c Config) validate() error {
	if err := c.validateShardLoad(); err != nil {
		return err
	}
	if c.Consumers < 1 || c.Shards < 1 {
		return fmt.Errorf("consumers and shards must be positive")
	}
	if err := c.validateRegions(); err != nil {
		return err
	}
	if c.Duration <= 0 {
		return fmt.Errorf("duration must be positive")
	}
	for name, d := range map[string]Duration{"warmup": c.Warmup, "reconnect_delay": c.ReconnectDelay} {
		if d < 0 {
			return fmt.Errorf("%s must be nonnegative", name)
		}
	}
	for name, rate := range map[string]float64{"relinquish_rate": c.RelinquishRate, "drain_rate": c.DrainRate, "reconnect_rate": c.ReconnectRate, "quit_rate": c.QuitRate} {
		if math.IsNaN(rate) || math.IsInf(rate, 0) || rate < 0 {
			return fmt.Errorf("%s must be finite and nonnegative", name)
		}
	}
	// Catch overflow before constructing timers.
	if int64(c.Warmup) > math.MaxInt64-int64(c.Duration) {
		return fmt.Errorf("total duration overflows")
	}

	return nil
}

func validRegion(region string) bool { return region != "" && strings.TrimSpace(region) == region }

func (c Config) validateRegions() error {
	remaining := c.Consumers
	seen := map[string]bool{}
	for i, group := range c.ConsumerRegions {
		if !validRegion(group.Region) || seen[group.Region] || group.Consumers < 1 || group.Consumers > remaining {
			return fmt.Errorf("consumer_regions group %d requires a unique region and positive count within consumers", i)
		}
		if group.DrainRate != nil && (math.IsNaN(*group.DrainRate) || math.IsInf(*group.DrainRate, 0) || *group.DrainRate < 0) {
			return fmt.Errorf("consumer_regions group %d drain_rate must be finite and nonnegative", i)
		}
		seen[group.Region] = true
		remaining -= group.Consumers
	}
	if len(c.ConsumerRegions) > 0 && remaining != 0 {
		return fmt.Errorf("consumer_regions counts must sum to consumers")
	}
	remaining = c.Shards
	for i, group := range c.ShardRegionPreferences {
		if group.Shards < 1 || group.Shards > remaining {
			return fmt.Errorf("shard_region_preferences group %d requires a positive count within shards", i)
		}
		remaining -= group.Shards
		seen = map[string]bool{}
		for _, region := range group.Regions {
			if !validRegion(region) || seen[region] {
				return fmt.Errorf("shard_region_preferences group %d requires unique nonblank regions", i)
			}
			seen[region] = true
		}
	}
	if len(c.ShardRegionPreferences) > 0 && remaining != 0 {
		return fmt.Errorf("shard_region_preferences counts must sum to shards")
	}
	return nil
}

// validateShardLoad requires percentages, rather than silently normalizing weights.
func (c Config) validateShardLoad() error {
	switch c.ShardLoadType {
	case Fixed, EvenDistribution:
		if len(c.ShardLoadDistribution) != 0 {
			return fmt.Errorf("shard_load_distribution requires CustomizedDistribution")
		}
	case CustomizedDistribution:
		if err := validateLoadDistribution(c.ShardLoadDistribution); err != nil {
			return fmt.Errorf("shard_load_distribution: %w", err)
		}
	default:
		return fmt.Errorf("shard_load_type must be Fixed, EvenDistribution, or CustomizedDistribution, got %q", c.ShardLoadType)
	}
	for i, group := range c.ShardRegionPreferences {
		if group.ShardLoadDistribution == nil {
			continue
		}
		if c.ShardLoadType != CustomizedDistribution {
			return fmt.Errorf("shard_region_preferences group %d shard_load_distribution requires CustomizedDistribution", i)
		}
		if err := validateLoadDistribution(group.ShardLoadDistribution); err != nil {
			return fmt.Errorf("shard_region_preferences group %d shard_load_distribution: %w", i, err)
		}
	}
	return nil
}

func validateLoadDistribution(percentages []float64) error {
	if len(percentages) != 10 {
		return fmt.Errorf("must contain exactly 10 percentages")
	}
	total := 0.0
	for i, percentage := range percentages {
		if math.IsNaN(percentage) || math.IsInf(percentage, 0) || percentage < 0 || percentage > 100 {
			return fmt.Errorf("bucket %d must be finite and in [0, 100]", i)
		}
		total += percentage
	}
	if math.Abs(total-100) > 1e-6 {
		return fmt.Errorf("percentages must sum to 100, got %g", total)
	}
	return nil
}
