package coordinator

import (
	"go.atoms.co/splitter/lib/service/location"
	"go.atoms.co/lib/mapx"
	"go.atoms.co/slicex"
	"go.atoms.co/lib/uuidx"
	"go.atoms.co/splitter/pkg/allocation"
	"go.atoms.co/splitter/pkg/core"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pb"
	"fmt"
	"time"
)

const (
	ShardLoad     allocation.Load = 10
	UnitShardLoad allocation.Load = 50
)

type (
	Allocation = allocation.Allocation[model.Shard, location.Location, model.ConsumerID, *Consumer]
	Placement  = allocation.Placement[model.Shard, location.Location, model.ConsumerID, *Consumer]
	Colocation = allocation.Colocation[model.Shard, location.Location, model.ConsumerID, *Consumer]
	Grant      = allocation.Grant[model.Shard, model.ConsumerID]
	Worker     = allocation.Worker[model.ConsumerID, *Consumer]
	WorkerInfo = allocation.WorkerInfo[model.ConsumerID, *Consumer]
	Work       = allocation.Work[model.Shard, location.Location]
	Assignment = allocation.Assignments[model.Shard, model.ConsumerID]
)

// TODO(herohde) 11/12/2023: intra-domain anti-affinity to spread out domains evenly. Similar to general LB.

func newAllocation(id location.InstanceID, tenant model.TenantInfo, info model.ServiceInfoEx, placements []core.InternalPlacementInfo, activation time.Time) *Allocation {
	return allocation.New(id, findPlacements(tenant, info), findColocations(info), findWork(info, placements), activation)
}

func updateAllocation(a *Allocation, tenant model.TenantInfo, info model.ServiceInfoEx, namedShards []model.Shard, placements []core.InternalPlacementInfo, activation time.Time) (*Allocation, []Grant) {
	return allocation.Update(a, findPlacements(tenant, info, namedShards...), findColocations(info), findWork(info, placements), activation)
}

// NamedShards handles named shard placement
type NamedShards struct {
	shards map[model.Shard]bool
}

func NewNamedShards(shards ...model.Shard) *NamedShards {
	return &NamedShards{shards: slicex.NewSet(shards...)}
}

func (r *NamedShards) ID() allocation.Rule {
	return "named-shards"
}

func (r *NamedShards) TryPlace(worker Worker, work Work) (allocation.Load, bool) {
	namedShard := r.shards[work.Unit]

	if len(worker.Data.Keys()) == 0 {
		if !namedShard {
			return 0, true // do not penalize normal shards for workers without named keys
		}
		return 20, true // penalty on named shards for workers without named keys
	}

	if !namedShard {
		return 0, false // do not schedule normal shards on a workers with named keys
	}

	for _, key := range worker.Data.Keys() {
		if work.Unit.Contains(key) {
			return 0, true // no penalty if shard contains a requested named key
		}
	}

	return 0, false // do not schedule non-matching named shards on a worker with named keys
}

type domainRegion struct {
	domain model.QualifiedDomainName
	region model.Region
}

// DomainState handles domain state placement
type DomainState struct {
	Domains map[model.DomainName]model.Domain
}

func NewDomainState(tenant model.TenantInfo, info model.ServiceInfoEx) *DomainState {
	banned := make(map[domainRegion]bool)

	t := tenant.Tenant().Operational().BannedRegions()
	s := info.Service().Operational().BannedRegions()
	for _, domain := range info.Domains() {
		d := domain.Operational().BannedRegions()

		for _, b := range [][]model.Region{t, s, d} {
			for _, region := range b {
				banned[domainRegion{domain: domain.Name(), region: region}] = true
			}
		}
	}
	return &DomainState{Domains: mapx.New(info.Domains(), model.Domain.ShortName)}
}

func (r *DomainState) ID() allocation.Rule {
	return "domain-state"
}

func (r *DomainState) TryPlace(worker Worker, work Work) (allocation.Load, bool) {
	if domain, ok := r.Domains[work.Unit.Domain.Domain]; ok && domain.State() != model.DomainActive {
		return 0, false // no placement allowed on non-active domains
	}
	return 0, true
}

// BannedWorkerRegion handles banned worker region placement
type BannedWorkerRegion struct {
	Banned map[domainRegion]bool
}

func NewBannedWorkerRegion(tenant model.TenantInfo, info model.ServiceInfoEx) *BannedWorkerRegion {
	banned := make(map[domainRegion]bool)

	t := tenant.Tenant().Operational().BannedRegions()
	s := info.Service().Operational().BannedRegions()
	for _, domain := range info.Domains() {
		d := domain.Operational().BannedRegions()

		for _, b := range [][]model.Region{t, s, d} {
			for _, region := range b {
				banned[domainRegion{domain: domain.Name(), region: region}] = true
			}
		}
	}
	return &BannedWorkerRegion{Banned: banned}
}

func (r *BannedWorkerRegion) ID() allocation.Rule {
	return "banned-worker-region"
}

func (r *BannedWorkerRegion) TryPlace(worker Worker, work Work) (allocation.Load, bool) {
	if r.Banned[domainRegion{domain: work.Unit.Domain, region: worker.Data.Instance().Location().Region}] {
		return 0, false // no placement allowed for banned worker regions
	}
	return 0, true
}

// RegionAffinity handles regional affinity preference.
type RegionAffinity struct {
	Overrides map[location.Region]location.Region
}

func NewRegionAffinity(info model.ServiceInfoEx) *RegionAffinity {
	return &RegionAffinity{Overrides: info.Service().Config().Overrides()}
}

func (r *RegionAffinity) ID() allocation.Rule {
	return "region-affinity"
}

func (r *RegionAffinity) TryPlace(worker Worker, work Work) (allocation.Load, bool) {
	pref := work.Data.Region

	if override, ok := r.Overrides[pref]; ok {
		pref = override
	}
	if pref == "" || worker.Data.Instance().Location().Region == pref {
		return 0, true
	}
	return 20, true
}

// AntiAffinity handles domain anti-affinity.
type AntiAffinity struct {
	Targets map[model.DomainName]bool
	Domains map[model.DomainName][]model.DomainName // directional domain -> targets: domain has the penalty
}

func NewAntiAffinity(info model.ServiceInfoEx) *AntiAffinity {
	domains := map[model.DomainName][]model.DomainName{}
	targets := map[model.DomainName]bool{}
	for _, d := range info.Domains() {
		for _, t := range d.Config().AntiAffinity() {
			targets[t] = true
			domains[d.ShortName()] = append(domains[d.ShortName()], t)
		}
	}
	return &AntiAffinity{Domains: domains, Targets: targets}
}

func (r *AntiAffinity) ID() allocation.Rule {
	return "anti-affinity"
}

// Colocate calculates anti-affinity load associated with individual work when allocating all the work together on the worker.
// This evaluation only penalizes the allocation iff there is a range overlap between shards that violates the anti-affinity rule.
func (r *AntiAffinity) Colocate(worker Worker, work map[model.Shard]Work) map[model.Shard]allocation.Load {
	if len(r.Targets) == 0 {
		return nil
	}
	ret := map[model.Shard]allocation.Load{}
	shards := map[model.DomainName][]model.Shard{}
	for shard := range work {
		if _, ok := r.Targets[shard.Domain.Domain]; ok {
			shards[shard.Domain.Domain] = append(shards[shard.Domain.Domain], shard)
		}
	}
	for shard := range work {
		load := 0
		for _, target := range r.Domains[shard.Domain.Domain] { // (anti-affinity) domain * shard <= work
			for _, s := range shards[target] {
				if s.IntersectsRange(shard) {
					load += 20
				}
			}
		}
		if load != 0 {
			ret[shard] = allocation.Load(load)
		}
	}
	return ret
}

func findPlacements(tenant model.TenantInfo, info model.ServiceInfoEx, namedShards ...model.Shard) []Placement {
	ret := slicex.New[Placement](NewRegionAffinity(info), NewDomainState(tenant, info), NewBannedWorkerRegion(tenant, info))
	if len(namedShards) > 0 {
		ret = append(ret, NewNamedShards(namedShards...))
	}
	return ret
}

func findColocations(info model.ServiceInfoEx) []Colocation {
	a := NewAntiAffinity(info)
	if len(a.Targets) == 0 {
		return nil
	}
	return slicex.New[Colocation](a)
}

func findWork(state model.ServiceInfoEx, placements []core.InternalPlacementInfo) []Work {
	var ret []Work

	m := mapx.New(placements, func(v core.InternalPlacementInfo) model.PlacementName {
		return v.Name().Placement
	})

	for _, domain := range state.Domains() {
		switch domain.Type() {
		case model.Unit:
			region := state.Info().Service().Config().Region()

			w := Work{
				Unit: model.Shard{
					Domain: domain.Name(),
					Type:   model.Unit,
				},
				Data: location.Location{Region: region},
				Load: UnitShardLoad, // use higher load for unit domains
			}
			ret = append(ret, w)

		case model.Global:
			shards := findShards(state.Info().Service(), domain)

			if placement, ok := domain.Config().Placement(); ok {
				// Dynamic region domain

				// TODO(herohde) 11/10/2023: we should perhaps split shards if region placement fall in the
				// middle of a shard, but for now we just use the start value to determine region.

				info, ok := m[placement]
				if !ok {
					continue // invalid: placement not present
				}

				provider := model.NewRegionProvider(info.InternalPlacement().Current().ToDistribution())

				for _, shard := range shards {
					region := provider.Find(model.Key(shard.From()))

					w := Work{
						Unit: model.Shard{
							Domain: domain.Name(),
							Type:   model.Global,
							From:   model.Key(shard.From()),
							To:     model.Key(shard.To()),
						},
						Data: location.Location{Region: region},
						Load: ShardLoad,
					}
					ret = append(ret, w)
				}

			} else {
				region := state.Info().Service().Config().Region()

				for _, shard := range shards {
					w := Work{
						Unit: model.Shard{
							Domain: domain.Name(),
							Type:   model.Global,
							From:   model.Key(shard.From()),
							To:     model.Key(shard.To()),
						},
						Data: location.Location{Region: region},
						Load: ShardLoad,
					}
					ret = append(ret, w)
				}
			}

		case model.Regional:
			shards := findShards(state.Info().Service(), domain)

			for _, r := range domain.Config().Regions() {
				for _, shard := range shards {
					w := Work{
						Unit: model.Shard{
							Region: r,
							Type:   model.Regional,
							Domain: domain.Name(),
							From:   model.Key(shard.From()),
							To:     model.Key(shard.To()),
						},
						Data: location.Location{Region: r},
						Load: ShardLoad,
					}
					ret = append(ret, w)
				}
			}

		default:
			// invalid domain
		}
	}

	return ret
}

func findShards(service model.Service, domain model.Domain) []uuidx.Range {
	n := domain.Config().ShardingPolicy().Shards()
	if n < 1 {
		n = service.Config().DefaultShardingPolicy().Shards()
	}
	shards, _ := uuidx.Split(uuidx.Domain, min(max(1, n), 1024))
	return shards
}

func toGrants(grants []Grant) []model.Grant {
	return slicex.Map(grants, toGrant)
}

func toGrant(g Grant) model.Grant {
	return model.NewGrant(g.ID, g.Unit, toGrantState(g.State, g.Mod), g.Expiration, g.Assigned)
}

func fromGrant(consumer *Consumer) func(g model.Grant) (Grant, error) {
	return func(g model.Grant) (Grant, error) {
		state, mod, ok := fromGrantState(g.State())
		if !ok {
			return Grant{}, fmt.Errorf("internal: unknown grant state: %v", g.State())
		}
		return allocation.NewGrant(g.ID(), state, mod, g.Shard(), consumer.ID(), g.Assigned(), g.Lease()), nil
	}
}

func toGrantState(s allocation.GrantState, mod allocation.GrantModifier) model.GrantState {
	switch s {
	case allocation.Active:
		return model.ActiveGrantState
	case allocation.Allocated:
		if mod == allocation.Loaded {
			return model.LoadedGrantState
		}
		return model.AllocatedGrantState
	case allocation.Revoked:
		if mod == allocation.Unloaded {
			return model.UnloadedGrantState
		}
		return model.RevokedGrantState
	default:
		return model.InvalidGrantState
	}
}

func fromGrantState(s model.GrantState) (allocation.GrantState, allocation.GrantModifier, bool) {
	switch s {
	case model.ActiveGrantState:
		return allocation.Active, allocation.None, true
	case model.AllocatedGrantState:
		return allocation.Allocated, allocation.None, true
	case model.LoadedGrantState:
		return allocation.Allocated, allocation.Loaded, true
	case model.RevokedGrantState:
		return allocation.Revoked, allocation.None, true
	case model.UnloadedGrantState:
		return allocation.Revoked, allocation.Unloaded, true

	default:
		return "", "", false
	}
}

func toGrantInfo(g Grant) model.GrantInfo {
	return model.WrapGrantInfo(&public_v1.ClusterMessage_GrantInfo{
		Id:    string(g.ID),
		Shard: g.Unit.ToProto(),
		State: toGrantState(g.State, g.Mod),
	})
}
