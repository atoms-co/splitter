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

	cache *ShardMap[GrantID, grantInfo]
}

func NewClusterMap(id ClusterID, assignments ...Assignment) *ClusterMap {
	ret := &ClusterMap{
		id:        id,
		consumers: map[ConsumerID]consumerInfo{},
		grants:    map[GrantID]grantInfo{},
		cache:     NewShardMap[GrantID, grantInfo](),
	}
	ret.initAssignments(assignments)

	return ret
}

func UpdateClusterMap(c *ClusterMap, msg ClusterMessage) (*ClusterMap, error) {
	version := msg.Version()
	timestamp := msg.Timestamp()

	switch {
	case msg.IsSnapshot():
		snapshot, _ := msg.Snapshot()

		id := ClusterID{
			Version:   version,
			Timestamp: timestamp,
		}
		id.Origin, _ = snapshot.Origin()

		return NewClusterMap(id, snapshot.Assignments()...), nil

	case msg.IsChange():
		change, _ := msg.Change()

		id := msg.ID()

		if !c.ID().IsNext(id, version) {
			return nil, fmt.Errorf("unexpected incremental update for %v: %v v%v", c.ID(), id, version)
		}

		ret := &ClusterMap{
			id:        c.ID().Next(timestamp),
			consumers: make(map[ConsumerID]consumerInfo, len(c.consumers)),
			grants:    make(map[GrantID]grantInfo, len(c.grants)),
			cache:     NewShardMap[GrantID, grantInfo](),
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

		return ret, nil

	default:
		return nil, fmt.Errorf("unexpected message type: %v", msg)
	}
}

func (c *ClusterMap) initAssignments(assignments []Assignment) {
	for _, a := range assignments {
		grants := a.Grants()
		consumer := a.Consumer()

		c.consumers[consumer.ID()] = consumerInfo{consumer: consumer, grants: grants}
		for _, g := range grants {
			info := grantInfo{consumer: consumer, grant: g}
			c.grants[g.ID()] = info
			c.cache.Write(g.Shard(), g.ID(), info)
		}
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
