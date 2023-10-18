package coordinator

import (
	"context"
	"atoms.co/lib-go/pkg/clock"
	"go.atoms.co/lib/log"
	"go.atoms.co/lib/mapx"
	"go.atoms.co/slicex"
	"go.atoms.co/splitter/pkg/core"
	"go.atoms.co/splitter/pkg/core/distribution"
	"go.atoms.co/splitter/pkg/model"
	"fmt"
	"time"
)

type Consumer struct {
	instance   model.Instance
	joined     time.Time
	expiration time.Time
}

func NewConsumer(instance model.Instance, joined time.Time) Consumer {
	return Consumer{instance: instance, joined: joined}
}

func (c Consumer) ID() model.InstanceID {
	return c.instance.ID()
}

func (c Consumer) Instance() model.Instance {
	return c.instance
}

func (c Consumer) Region() model.Region {
	return model.Region(c.instance.Location().Region)
}

func (c Consumer) Joined() time.Time {
	return c.joined
}

func (c Consumer) Expiration() time.Time {
	return c.expiration
}

func (c Consumer) String() string {
	return fmt.Sprintf("consumer{instance=%v, joined=%v, expiration=%v}", c.instance, c.joined, c.expiration)
}

type ShardManager struct {
	cl clock.Clock

	distribution *distribution.Distribution[model.Shard, model.Grant]

	snapshot *[]model.Assignment

	removed  map[model.GrantID]bool
	assigned map[model.GrantID]model.Instance
}

func NewShardManager(ctx context.Context, cl clock.Clock, state core.State) *ShardManager {
	mgr := &ShardManager{
		removed:  map[model.GrantID]bool{},
		assigned: map[model.GrantID]model.Instance{},
	}
	shards := createShards(ctx, state)
	mgr.distribution = distribution.NewDistribution[model.Shard, model.Grant](mgr, cl.Now().Add(leaseDuration), shards)
	return mgr
}

func (m *ShardManager) Connect(consumer Consumer, limit int, limitCutoff time.Time) error {
	err := m.distribution.Connect(consumer, limit, limitCutoff)
	if err != nil {
		return err
	}
	return nil
}

func (m *ShardManager) Disconnect(consumer Consumer) error {
	grants, _, err := m.distribution.Disconnect(m.cl.Now(), consumer.ID())
	if err != nil {
		return err
	}
	for _, g := range grants {
		m.removeGrant(g.ID)
	}
	return nil
}

func (m *ShardManager) Release(consumer Consumer, grant model.GrantID) error {
	ok, err := m.distribution.Release(consumer.ID(), distribution.GrantID(grant), m.cl.Now())
	if err != nil {
		return err
	}
	if ok {
		m.removeGrant(grant)
	}
	return nil
}

func (m *ShardManager) Snapshot() model.ClusterSnapshot {
	if m.snapshot == nil {
		var assignments []model.Assignment
		for _, consumer := range m.distribution.Consumers() {
			assignments = append(assignments, model.NewAssignment(consumer.Instance(), m.distribution.Assigned(consumer.ID())))
		}
		m.snapshot = &assignments
	}

	return model.NewClusterSnapshot(*m.snapshot)
}

func (m *ShardManager) DiscardClusterUpdate(ctx context.Context) (model.ClusterUpdate, bool) {
	if m.snapshot != nil {
		return model.ClusterUpdate{}, false
	}

	var consumerGrants map[model.InstanceID][]model.Grant
	var consumers map[model.InstanceID]model.Instance
	for id, consumer := range m.assigned {
		grant, ok := m.distribution.Grant(distribution.GrantID(id))
		if !ok {
			log.Errorf(ctx, "Unassigned grant %v is present in the assignment to consumer %v", grant, consumer)
			continue
		}
		cid := consumer.ID()
		consumers[cid] = consumer
		consumerGrants[cid] = append(consumerGrants[cid], grant)
	}

	var assignments []model.Assignment
	for cid, grants := range consumerGrants {
		assignments = append(assignments, model.NewAssignment(consumers[cid], grants))
	}

	removed := mapx.Keys(m.removed)

	m.assigned = map[model.GrantID]model.Instance{}
	m.removed = map[model.GrantID]bool{}

	return model.NewClusterUpdate(assignments, removed), true
}

func (m *ShardManager) NewGrant(shard model.Shard, state distribution.GrantState, assigned time.Time, expiration time.Time) model.Grant {
	return model.Grant{
		ID:       model.NewGrantID(),
		Shard:    shard,
		Assigned: assigned,
		Lease:    expiration,
	}
}

func (m *ShardManager) GrantWithShard(g model.Grant, shard model.Shard) model.Grant {
	g.Shard = shard
	return g
}

func (m *ShardManager) GrantWithAssigned(g model.Grant, assigned time.Time) model.Grant {
	g.Assigned = assigned
	return g
}

func (m *ShardManager) GrantWithExpiration(g model.Grant, expiration time.Time) model.Grant {
	g.Lease = expiration
	return g
}

func (m *ShardManager) GrantWithState(g model.Grant, state distribution.GrantState) model.Grant {
	newState := model.InvalidGrant
	switch state {
	case distribution.AllocatedGrant:
		newState = model.AllocatedGrant
	case distribution.ActiveGrant:
		newState = model.ActiveGrant
	case distribution.RevokedGrant:
		newState = model.RevokedGrant
	}
	g.State = newState
	return g
}

func (m *ShardManager) GrantShard(g model.Grant) model.Shard {
	return g.Shard
}

func (m *ShardManager) GrantAssigned(g model.Grant) time.Time {
	return g.Assigned
}

func (m *ShardManager) GrantExpiration(g model.Grant) time.Time {
	return g.Lease
}

func (m *ShardManager) GrantState(g model.Grant) distribution.GrantState {
	switch g.State {
	case model.AllocatedGrant:
		return distribution.AllocatedGrant
	case model.ActiveGrant:
		return distribution.ActiveGrant
	case model.RevokedGrant:
		return distribution.RevokedGrant
	default:
		return distribution.UnknownGrant
	}
}

func (m *ShardManager) GrantID(g model.Grant) distribution.GrantID {
	return distribution.GrantID(g.ID)
}

func (m *ShardManager) ShardsEqual(s1 model.Shard, s2 model.Shard) bool {
	return s1 == s2
}

func (m *ShardManager) removeGrant(g model.GrantID) {
	m.removed[g] = true
	delete(m.assigned, g)
	m.snapshot = nil
}

func (m *ShardManager) assignGrant(g model.GrantID, consumer model.Instance) {
	delete(m.removed, g)
	m.assigned[g] = consumer
	m.snapshot = nil
}

func createShards(ctx context.Context, state core.State) []model.Shard {
	tenant := state.Tenant().Tenant()
	placements := state.Placements()
	return slicex.FlatMap(state.Domains(), func(info model.DomainInfo) []model.Shard {
		return createDomainShards(ctx, tenant, info.Domain(), placements)
	})
}

func createDomainShards(ctx context.Context, tenant model.Tenant, domain model.Domain, placements []core.InternalPlacementInfo) []model.Shard {
	var shards []model.Shard

	if domain.State() == model.DomainSuspended {
		return shards
	}

	switch domain.Type() {
	case model.Unit:
		r, _ := tenant.Region()
		shards = append(shards, model.Shard{
			Region: r,
			Domain: domain.Name(),
			To:     model.ZeroKey,
			From:   model.MaxKey,
		})
	case model.Global:
		if name, ok := domain.Config().Placement(); ok {
			placement, ok := slicex.First(placements, func(info core.InternalPlacementInfo) bool {
				pname := info.InternalPlacement().Name()
				return pname.Tenant == tenant.Name() && pname.Placement == name
			})
			if ok {
				if placement.InternalPlacement().State() == core.PlacementActive {
					shards = append(shards, createPlacementShards(domain, placement.InternalPlacement())...)
				}
			}
		} else {
			r, _ := tenant.Region()
			shards = append(shards, model.NewShards(r, domain.Name(), shardingPolicy(tenant, domain).Shards())...)
		}
	case model.Regional:
		domainShards := slicex.FlatMap(domain.Config().Regions(), func(r model.Region) []model.Shard {
			return model.NewShards(r, domain.Name(), domain.Config().ShardingPolicy().Shards())
		})
		shards = append(shards, domainShards...)
	default:
		log.Errorf(ctx, "Ignoring unsupported domain type for domain: %v", domain)
	}

	return shards
}

func createPlacementShards(domain model.Domain, placement core.InternalPlacement) []model.Shard {
	var shards []model.Shard
	// TODO: implement
	return shards
}

func shardingPolicy(tenant model.Tenant, domain model.Domain) model.ShardingPolicy {
	if domain.Config().ShardingPolicy() == model.WrapShardingPolicy(nil) {
		return tenant.Config().DefaultShardingPolicy()
	}
	return domain.Config().ShardingPolicy()
}
