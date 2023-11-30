package coordinator

import (
	"go.atoms.co/splitter/lib/service/location"
	"go.atoms.co/lib/mapx"
	"go.atoms.co/lib/mathx"
	"go.atoms.co/slicex"
	"go.atoms.co/lib/uuidx"
	"go.atoms.co/splitter/pkg/allocation"
	"go.atoms.co/splitter/pkg/core"
	"go.atoms.co/splitter/pkg/model"
	"fmt"
	"time"
)

type (
	Allocation = allocation.Allocation[model.Shard, location.Location, model.ConsumerID, model.Consumer]
	Placement  = allocation.Placement[model.Shard, location.Location, model.ConsumerID, model.Consumer]
	Colocation = allocation.Colocation[model.Shard, location.Location, model.ConsumerID, model.Consumer]
	Grant      = allocation.Grant[model.Shard, model.ConsumerID]
	Worker     = allocation.Worker[model.ConsumerID, model.Consumer]
	WorkerInfo = allocation.WorkerInfo[model.ConsumerID, model.Consumer]
	Work       = allocation.Work[model.Shard, location.Location]
	Assignment = allocation.Assignments[model.Shard, model.ConsumerID]
)

// TODO(herohde) 11/11/2023: need custom region-affinity that returns true if in the same region or buddy
// region, with the exception thaeastus2 as an outpost has its CRDB leaseholders in northcentralus. Does not
// apply to all services/domains, so it should be a configuration option.

// TODO(herohde) 11/12/2023: intra-domain anti-affinity to spread out domains evenly. Similar to general LB.

var (
	regionAffinity = allocation.NewPreference("region-affinity", 20, HasRegionAffinity)
)

func HasRegionAffinity(worker Worker, work Work) bool {
	return work.Data.Region == "" || worker.Data.Location().Region == work.Data.Region
}

func newAllocation(id location.InstanceID, info model.ServiceInfoEx, placements []core.InternalPlacementInfo, activation time.Time) *Allocation {
	return allocation.New(id, findPlacements(info), findColocations(info), findWork(info, placements), activation)
}

func updateAllocation(a *Allocation, info model.ServiceInfoEx, placements []core.InternalPlacementInfo, activation time.Time) (*Allocation, []Grant) {
	return allocation.Update(a, findPlacements(info), findColocations(info), findWork(info, placements), activation)
}

// control handles placement control.
type control struct {
	domains map[model.DomainName]model.Domain
}

func newControl(info model.ServiceInfoEx) *control {
	return &control{domains: mapx.New(info.Domains(), model.Domain.ShortName)}
}

func (c *control) ID() allocation.Rule {
	return "control"
}

func (c *control) TryPlace(worker Worker, work Work) (allocation.Load, bool) {
	if domain, ok := c.domains[work.Unit.Domain.Domain]; ok && domain.State() != model.DomainActive {
		return 0, false // no placement allowed
	}
	return 0, true
}

// affinity handles domain anti-affinity.
type affinity struct {
	domains map[model.DomainName][]model.DomainName // directional domain -> targets: domain has the penalty
	targets map[model.DomainName]bool               // target filter
}

func newAffinity(info model.ServiceInfoEx) *affinity {
	return &affinity{
		domains: map[model.DomainName][]model.DomainName{},
		targets: map[model.DomainName]bool{},
	}
	// TODO(herohde) 11/12/2023: implement
}

func (c *affinity) ID() allocation.Rule {
	return "anti-affinity"
}

func (c *affinity) Colocate(worker Worker, work map[model.Shard]Work) map[model.Shard]allocation.Load {
	if len(c.domains) == 0 {
		return nil
	}

	// TODO(herohde) 11/12/2023: implement
	return nil
}

func findPlacements(info model.ServiceInfoEx) []Placement {
	return slicex.New[Placement](regionAffinity, newControl(info))
}

func findColocations(info model.ServiceInfoEx) []Colocation {
	return slicex.New[Colocation](newAffinity(info))
}

func findWork(state model.ServiceInfoEx, placements []core.InternalPlacementInfo) []Work {
	var ret []Work

	m := mapx.New(placements, func(v core.InternalPlacementInfo) model.PlacementName {
		return v.Name().Placement
	})

	for _, domain := range state.Domains() {
		switch domain.Type() {
		case model.Unit:
			region, _ := state.Info().Service().Region()

			w := Work{
				Unit: model.Shard{
					Domain: domain.Name(),
					Type:   model.Unit,
				},
				Data: location.Location{Region: region},
				Load: 50, // use higher load for unit domains
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
						Load: 10,
					}
					ret = append(ret, w)
				}

			} else {
				region, _ := state.Info().Service().Region()

				for _, shard := range shards {
					w := Work{
						Unit: model.Shard{
							Domain: domain.Name(),
							Type:   model.Global,
							From:   model.Key(shard.From()),
							To:     model.Key(shard.To()),
						},
						Data: location.Location{Region: region},
						Load: 10,
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
						Load: 10,
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
	shards, _ := uuidx.Split(uuidx.Domain, mathx.Min(mathx.Max(1, n), 1024))
	return shards
}

func grantID(g Grant) model.GrantID {
	return model.GrantID(g.ID)
}

func toGrants(grants []Grant) []model.Grant {
	return slicex.Map(grants, toGrant)
}

func toGrant(g Grant) model.Grant {
	return model.Grant{
		ID:       model.GrantID(g.ID),
		Shard:    g.Unit,
		State:    toGrantState(g.State),
		Lease:    g.Expiration,
		Assigned: g.Assigned,
	}
}

func fromGrant(g model.Grant) (Grant, error) {
	s, ok := fromGrantState(g.State)
	if !ok {
		return Grant{}, fmt.Errorf("internal: unknown grant state: %v", g.State)
	}
	return Grant{
		ID:         allocation.GrantID(g.ID),
		Unit:       g.Shard,
		State:      s,
		Expiration: g.Lease,
		Assigned:   g.Assigned,
	}, nil
}

func toGrantState(s allocation.GrantState) model.GrantState {
	switch s {
	case allocation.Active:
		return model.ActiveGrant
	case allocation.Allocated:
		return model.AllocatedGrant
	case allocation.Revoked:
		return model.RevokedGrant
	default:
		return model.InvalidGrant
	}
}

func fromGrantState(s model.GrantState) (allocation.GrantState, bool) {
	switch s {
	case model.ActiveGrant:
		return allocation.Active, true
	case model.AllocatedGrant:
		return allocation.Allocated, true
	case model.RevokedGrant:
		return allocation.Revoked, true
	default:
		return "", false
	}
}

func toCluster(a *Allocation, id model.ClusterId, version int) (model.Cluster, error) {
	workers := a.Workers()
	var consumers []model.Consumer
	allGrants := map[model.ConsumerID][]model.GrantInfo{}

	for _, w := range workers {
		consumers = append(consumers, w.Instance.Data)
		allGrants[w.Instance.ID] = assignmentGrants(a.Assigned(w.Instance.ID))
	}

	return model.NewCluster(id, version, consumers, allGrants)
}

func assignmentGrants(a Assignment) []model.GrantInfo {
	grants := slicex.Map(a.Allocated, toGrantInfo)
	grants = append(grants, slicex.Map(a.Active, toGrantInfo)...)
	grants = append(grants, slicex.Map(a.Revoked, toGrantInfo)...)
	return grants
}

func toGrantInfo(g Grant) model.GrantInfo {
	return model.GrantInfo{
		ID:    model.GrantID(g.ID),
		Shard: g.Unit,
		State: toGrantState(g.State),
	}
}
