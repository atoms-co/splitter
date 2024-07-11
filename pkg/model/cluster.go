package model

import (
	"go.atoms.co/splitter/lib/service/location"
	"go.atoms.co/lib/mapx"
	"go.atoms.co/slicex"
	"fmt"
	"sync"
	"time"
)

// ClusterID identifies cluster version and origin information.
type ClusterID struct {
	Origin    location.Instance
	Version   int
	Timestamp time.Time
}

func NewClusterID(origin location.Instance, now time.Time) ClusterID {
	return ClusterID{
		Origin:    origin,
		Version:   1,
		Timestamp: now,
	}
}

// Next returns a ClusterID for the next incremental update.
func (c ClusterID) Next(now time.Time) ClusterID {
	ret := c
	ret.Version++
	ret.Timestamp = now
	return ret
}

// IsNext returns true if the id and version matches the next incremental update.
func (c ClusterID) IsNext(id location.InstanceID, version int) bool {
	return c.Origin.ID() == id && c.Version+1 == version
}

func (c ClusterID) String() string {
	return fmt.Sprintf("%v[origin=%v, updated=+%v]", c.Version, c.Origin, c.Timestamp.Sub(c.Origin.Created()).Round(time.Millisecond))
}

// Cluster contains information about all consumers and grants in the work distribution process. Immutable.
type Cluster interface {
	ID() ClusterID

	// Consumers returns all the consumers joined the work distribution process (even without any assigned shards)
	Consumers() []Consumer
	// Consumer returns consumer and grants for the given consumer id, if present.
	Consumer(id ConsumerID) (Consumer, []GrantInfo, bool)
	// Grant returns consumer and grant for the given grant id, if present.
	Grant(id GrantID) (Consumer, GrantInfo, bool)

	// Lookup returns the consumer and grant, if any, for the given key, with the constraint that the grant
	// state is the first present in the given list. If none are provided, lookup implicitly uses the default
	// notion of ownership under possible transitional states: [Active, Revoked, Loaded, Unloaded].
	Lookup(key QualifiedDomainKey, states ...GrantState) (Consumer, GrantInfo, bool)
}

type consumerInfo struct {
	consumer Consumer
	grants   []GrantInfo
}

func (c consumerInfo) String() string {
	return fmt.Sprintf("%v{%v}", c.consumer, c.grants)
}

type grantInfo struct {
	consumer Consumer
	grant    GrantInfo
}

func (g grantInfo) String() string {
	return fmt.Sprintf("%v{%v}", g.consumer, g.grant)
}

// ClusterMap is lookup-optimized Cluster representation, updated by ClusterMessages. Immutable.
type ClusterMap struct {
	id        ClusterID
	consumers map[ConsumerID]consumerInfo
	grants    map[GrantID]grantInfo
	shards    map[Shard]map[GrantID]bool

	cache *ShardMap[GrantID, grantInfo]
}

func NewClusterMap(id ClusterID, shards []Shard, assignments []Assignment) *ClusterMap {
	ret := &ClusterMap{
		id:        id,
		consumers: map[ConsumerID]consumerInfo{},
		grants:    map[GrantID]grantInfo{},
		cache:     NewShardMap[GrantID, grantInfo](),
		shards:    make(map[Shard]map[GrantID]bool, len(shards)),
	}

	for _, shard := range shards {
		ret.shards[shard] = map[GrantID]bool{}
	}

	ret.initAssignments(assignments)
	ret.addShardGrants()

	return ret
}

func UpdateClusterMap(c *ClusterMap, msg ClusterMessage) (*ClusterMap, error) {
	version := msg.Version()
	timestamp := msg.Timestamp()

	switch {
	case msg.IsSnapshot():
		snapshot, _ := msg.Snapshot()

		clusterID := ClusterID{
			Version:   version,
			Timestamp: timestamp,
		}
		clusterID.Origin, _ = snapshot.Origin()

		shards, err := snapshot.Shards()
		if err != nil {
			return nil, fmt.Errorf("unable to parse shards in cluster snapshot: %v", err)
		}

		upd := NewClusterMap(clusterID, shards, snapshot.Assignments())
		if len(upd.shards) == 0 { // TODO (styurin, 7/2/2024): remove when service sends shards in snapshot
			// Use current assignments to populate shards if snapshot is empty
			for shard := range c.shards {
				upd.shards[shard] = map[GrantID]bool{}
			}
		}

		old := c.collectOldValidGrants(upd)

		if len(old) > 0 {
			// Retain old grants that can still be assigned, but unknown to the coordinator
			for gid := range old {
				info := c.grants[gid]
				upd.grants[gid] = info
				if consumer, ok := upd.consumers[info.consumer.ID()]; ok {
					consumer.grants = append(consumer.grants, info.grant)
					upd.consumers[info.consumer.ID()] = consumer
				} else {
					upd.consumers[info.consumer.ID()] = consumerInfo{consumer: info.consumer, grants: []GrantInfo{info.grant}}
				}
				upd.cache.Write(info.grant.Shard(), info.grant.ID(), info)
			}
			upd.addShardGrants()
		}

		return upd, nil

	case msg.IsChange():
		change, _ := msg.Change()

		id := msg.ID()

		if !c.ID().IsNext(id, version) {
			return nil, fmt.Errorf("unexpected incremental update for %v: %v v%v", c.ID(), id, version)
		}

		var shards []Shard
		if change.HasShards() {
			var err error
			shards, err = change.Shards().Shards()
			if err != nil {
				return nil, fmt.Errorf("unable to parse shards in cluster change: %v", err)
			}
		} else {
			shards = c.Shards() // Shards have not changed, use the current ones
		}

		ret := &ClusterMap{
			id:        c.ID().Next(timestamp),
			consumers: make(map[ConsumerID]consumerInfo, len(c.consumers)),
			grants:    make(map[GrantID]grantInfo, len(c.grants)),
			cache:     NewShardMap[GrantID, grantInfo](),
			shards:    make(map[Shard]map[GrantID]bool, len(shards)),
		}

		for _, shard := range shards {
			ret.shards[shard] = map[GrantID]bool{}
		}

		// (1) Add new assignments

		ret.initAssignments(change.Assign().Assignments())

		// (2) Copy over retained or updated values

		upd := mapx.New(change.Update().Grants(), GrantInfo.ID)
		rem := mapx.New(change.Unassign().Grants(), idFn[GrantID])
		del := mapx.New(change.Remove().Consumers(), idFn[ConsumerID])

		for cid, info := range c.consumers {
			if _, ok := del[cid]; ok {
				continue // skip: consumer removed
			}

			grants := make([]GrantInfo, 0, len(info.grants))

			for _, g := range info.grants {
				if _, ok := rem[g.ID()]; ok {
					continue // skip: grant removed
				}

				keep := grantInfo{consumer: info.consumer, grant: g}
				if v, ok := upd[g.ID()]; ok {
					keep.grant = v // grant updated
				}

				ret.grants[g.ID()] = keep
				ret.cache.Write(g.Shard(), g.ID(), keep)
				grants = append(grants, keep.grant)
			}

			if fresh, ok := ret.consumers[cid]; ok {
				grants = append(grants, fresh.grants...)
			}
			ret.consumers[cid] = consumerInfo{consumer: info.consumer, grants: grants}
		}

		ret.addShardGrants()

		return ret, nil

	default:
		return nil, fmt.Errorf("unexpected message type: %v", msg)
	}
}

// collectOldValidGrants collect grants from the current cluster that can still be valid, but not present
// in the new cluster
func (c *ClusterMap) collectOldValidGrants(upd *ClusterMap) map[GrantID]bool {
	old := map[GrantID]bool{}
	for gid, oldInfo := range c.grants {
		if _, ok := upd.grants[gid]; ok {
			// Grant is already present in the new cluster. Skip
			continue
		}

		shard := oldInfo.grant.Shard()
		newGrants, ok := upd.shards[shard]
		if !ok {
			// The old shard is unknown, most likely after resharding. Discard the grant.
			continue
		}

		if len(newGrants) == 0 {
			// Shard has no assignments in the new cluster. Keep grant for potential revival.
			old[gid] = true
			continue
		}

		oldShards := c.shards[shard]
		// When old cluster has two assignments (in allocated and revoked state) and new cluster has one assignment
		// (in matching state), then keep the missing assignment for potential revival.
		if len(oldShards) == 2 && len(newGrants) == 1 {
			newID, _, _ := mapx.GetOnly(newGrants)
			if oldShards[newID] && grantStatesMatch(c, upd, newID) {
				// The new grant is assigned to the same shard in the old cluster and states of that grant
				// are matching in both clusters. Keep the other grant from old grants.
				old[gid] = true
			}
		}
	}
	return old
}

// grantStatesMatch verifies that states of the given grant match in two clusters. Grants in substates of allocated
// and revoked are considered matching.
func grantStatesMatch(c1 *ClusterMap, c2 *ClusterMap, gid GrantID) bool {
	info1 := c1.grants[gid]
	info2 := c2.grants[gid]

	if info1.grant.State() == info2.grant.State() {
		return true
	}

	allocated1 := IsAllocatedOrLoaded(info1.grant.State())
	revoked1 := IsRevokedOrUnloaded(info1.grant.State())
	allocated2 := IsAllocatedOrLoaded(info2.grant.State())
	revoked2 := IsRevokedOrUnloaded(info2.grant.State())

	return (allocated1 && allocated2) || (revoked1 && revoked2)
}

func (c *ClusterMap) initAssignments(assignments []Assignment) {
	for _, a := range assignments {
		grants := a.Grants()
		consumer := a.Consumer()

		c.consumers[consumer.ID()] = consumerInfo{consumer: consumer, grants: grants}
		// TODO (styurin, 7/9/2024): check grants to use valid shards when service sends shards in snapshot
		for _, g := range grants {
			info := grantInfo{consumer: consumer, grant: g}
			c.grants[g.ID()] = info
			c.cache.Write(g.Shard(), g.ID(), info)
		}
	}
}

func (c *ClusterMap) addShardGrants() {
	for _, ginfo := range c.grants {
		shard := ginfo.grant.Shard()
		// Check for shard being present. Shard may be missing if cluster map was created with empty shards.
		if _, ok := c.shards[shard]; !ok {
			c.shards[shard] = map[GrantID]bool{}
		}
		c.shards[shard][ginfo.grant.ID()] = true
	}
}

func (c *ClusterMap) ID() ClusterID {
	return c.id
}

func (c *ClusterMap) Consumers() []Consumer {
	return mapx.MapValues(c.consumers, func(v consumerInfo) Consumer {
		return v.consumer
	})
}

func (c *ClusterMap) Consumer(id ConsumerID) (Consumer, []GrantInfo, bool) {
	info, ok := c.consumers[id]
	return info.consumer, info.grants, ok
}

func (c *ClusterMap) Grant(id GrantID) (Consumer, GrantInfo, bool) {
	info, ok := c.grants[id]
	return info.consumer, info.grant, ok
}

func (c *ClusterMap) Lookup(key QualifiedDomainKey, states ...GrantState) (Consumer, GrantInfo, bool) {
	if len(states) == 0 {
		states = []GrantState{ActiveGrantState, RevokedGrantState, LoadedGrantState, UnloadedGrantState}
	}

	byState := mapx.New(c.cache.Lookup(key), func(v ShardKV[GrantID, grantInfo]) GrantState {
		return v.V.grant.State()
	})

	for _, state := range states {
		if elm, ok := byState[state]; ok {
			return elm.V.consumer, elm.V.grant, true
		}
	}
	return Consumer{}, GrantInfo{}, false
}

func (c *ClusterMap) Assignments() []Assignment {
	return slicex.Map(c.Consumers(), func(consumer Consumer) Assignment {
		_, grants, _ := c.Consumer(consumer.ID())
		return NewAssignment(consumer, grants...)
	})
}

func (c *ClusterMap) Shards() []Shard {
	return mapx.Keys(c.shards)
}

func (c *ClusterMap) ShardGrants(shard Shard) ([]GrantID, bool) {
	if shards, ok := c.shards[shard]; ok {
		return mapx.Keys(shards), true
	}
	return nil, false
}

func (c *ClusterMap) String() string {
	return fmt.Sprintf("%v{%v}", c.id, c.consumers)
}

func idFn[T any](t T) T {
	return t
}

type grantMapElm[T any] struct {
	state GrantState
	value T
}

// GrantMap is a map Grant -> T optimized for domain key lookup. Thread-safe.
type GrantMap[T any] struct {
	m  *ShardMap[GrantID, grantMapElm[T]]
	mu sync.RWMutex
}

func NewGrantMap[T any]() *GrantMap[T] {
	return &GrantMap[T]{
		m: NewShardMap[GrantID, grantMapElm[T]](),
	}
}

func (m *GrantMap[T]) Domains() []QualifiedDomainName {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.m.Domains()
}

func (m *GrantMap[T]) Domain(name QualifiedDomainName) []T {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return slicex.Map(m.m.Domain(name), func(t ShardKV[GrantID, grantMapElm[T]]) T {
		return t.V.value
	})
}

func (m *GrantMap[T]) Loaded(id GrantID, shard Shard, value T) {
	m.Write(id, shard, LoadedGrantState, value)
}

func (m *GrantMap[T]) Activate(id GrantID, shard Shard, value T) {
	m.Write(id, shard, ActiveGrantState, value)
}

func (m *GrantMap[T]) Revoke(id GrantID, shard Shard, value T) {
	m.Write(id, shard, RevokedGrantState, value)
}

func (m *GrantMap[T]) Unloaded(id GrantID, shard Shard, value T) {
	m.Write(id, shard, UnloadedGrantState, value)
}

func (m *GrantMap[T]) Write(id GrantID, shard Shard, state GrantState, value T) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.m.Write(shard, id, grantMapElm[T]{state: state, value: value})
}

func (m *GrantMap[T]) Delete(id GrantID, shard Shard) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.m.Delete(shard, id)
}

func (m *GrantMap[T]) Lookup(key QualifiedDomainKey, states ...GrantState) (T, bool) {
	if len(states) == 0 {
		states = []GrantState{ActiveGrantState, RevokedGrantState, LoadedGrantState, UnloadedGrantState}
	}

	m.mu.RLock()
	candidates := m.m.Lookup(key)
	m.mu.RUnlock()

	byState := mapx.New(candidates, func(v ShardKV[GrantID, grantMapElm[T]]) GrantState {
		return v.V.state
	})

	for _, state := range states {
		if elm, ok := byState[state]; ok {
			return elm.V.value, true
		}
	}

	var zero T
	return zero, false
}

// DomainShards returns a list of shards for the given domain
func DomainShards(cluster Cluster, domain QualifiedDomainName) []Shard {
	shards := map[Shard]bool{}

	for _, consumer := range cluster.Consumers() {
		_, grants, ok := cluster.Consumer(consumer.ID())
		if !ok {
			continue
		}
		for _, g := range grants {
			if g.Shard().Domain == domain {
				shards[g.Shard()] = true
			}
		}
	}
	return mapx.Keys(shards)
}
