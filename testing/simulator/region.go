package simulator

import (
	"fmt"

	"go.atoms.co/splitter/pkg/model"
)

func defaultRegion(region string) string {
	if region == "" {
		return "centralus"
	}
	return region
}

func (c Config) initialConsumerRegion(slot int) string {
	for _, group := range c.ConsumerRegions {
		if slot < group.Consumers {
			return group.Region
		}
		slot -= group.Consumers
	}
	return "centralus"
}

func (c Config) drainRateForRegion(region string) float64 {
	for _, group := range c.ConsumerRegions {
		if group.Region == region && group.DrainRate != nil {
			return *group.DrainRate
		}
	}
	return c.DrainRate
}

// Preference groups use native global domains, which support a set of equally
// preferred regions without replicating the shard count in every region. The
// legacy scenario keeps its original regional domain and shard identities.
func (s *simulation) initializeDomains() []model.Domain {
	groups := s.cfg.ShardRegionPreferences
	legacy := len(groups) == 0
	if legacy {
		groups = []ShardRegionPreference{{Shards: s.cfg.Shards, Regions: []string{"centralus"}}}
	}
	domains := make([]model.Domain, 0, len(groups))
	for i, group := range groups {
		name, kind := model.DomainName(fmt.Sprintf("shards-%d", i)), model.Global
		if legacy {
			name, kind = "shards", model.Regional
		}
		regions := make([]model.Region, len(group.Regions))
		for j, region := range group.Regions {
			regions[j] = model.Region(region)
		}
		domain, err := model.NewDomain(model.QualifiedDomainName{Service: s.service, Domain: name}, kind, s.start,
			model.WithDomainConfig(model.NewDomainConfig(model.WithDomainShardingPolicy(model.NewShardingPolicy(group.Shards)), model.WithDomainRegions(regions...))))
		if err != nil {
			s.t.Fatal(err)
		}
		domains = append(domains, domain)
	}
	return domains
}
