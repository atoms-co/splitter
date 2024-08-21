package model

import (
	"context"
	"go.atoms.co/splitter/lib/service/location"
	"go.atoms.co/lib/log"
	"go.atoms.co/lib/mapx"
	"go.atoms.co/slicex"
	"fmt"
	"sort"
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

	// Shards returns a list of all shards in the cluster
	Shards() []Shard

	// Lookup returns the consumer and grant, if any, for the given key, with the constraint that the grant
	// state is the first present in the given list. If none are provided, lookup implicitly uses the default
	// notion of ownership under possible transitional states: [Active, Revoked, Loaded, Unloaded].
	Lookup(key QualifiedDomainKey, states ...GrantState) (Consumer, GrantInfo, bool)
}

type consumerInfo struct {
	consumer Consumer
	grants   []GrantInfo
	version  int // set when grant is copied from the old cluster. Current consumers have version 0.
}

// Retained returns true if the consumer was copied from an old cluster map during applying a snapshot operation,
// and it was not listed by the coordinator since then.
func (c consumerInfo) Retained() bool {
	return c.version > 0
}

func (c consumerInfo) String() string {
	return fmt.Sprintf("%v{%v}", c.consumer, c.grants)
}

type grantInfo struct {
	consumer Consumer
	grant    GrantInfo
	version  int // set when grant is copied from the old cluster. Current grants have version 0.
}

// Retained returns true if the grant was copied from an old cluster map during applying a snapshot operation,
// and it was not listed by the coordinator since then.
func (g grantInfo) Retained() bool {
	return g.version > 0
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

func NewClusterMap(id ClusterID, shards []Shard) *ClusterMap {
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

	return ret
}

func UpdateClusterMap(ctx context.Context, c *ClusterMap, msg ClusterMessage) (*ClusterMap, error) {
	timestamp := msg.Timestamp()

	switch {
	case msg.IsSnapshot():
		snapshot, _ := msg.Snapshot()

		clusterID := ClusterID{
			Version:   msg.Version(),
			Timestamp: timestamp,
		}
		clusterID.Origin, _ = snapshot.Origin()

		shards, err := snapshot.Shards()
		if err != nil {
			return nil, fmt.Errorf("unable to parse shards in cluster snapshot: %v", err)
		}
		assignments := snapshot.Assignments()
		if err = c.validateSnapshot(shards, assignments); err != nil {
			return nil, fmt.Errorf("invalid cluster snapshot: %v", err)
		}

		ret := NewClusterMap(clusterID, shards)
		ret.initAssignments(assignments)

		// Copy old valid assignments

		for _, info := range c.grants {
			if _, ok := ret.grants[info.grant.ID()]; ok {
				continue // skip: grant already assigned
			}
			if info.version == 0 {
				info.version = c.id.Version
			}

			consumer, _ := c.consumers[info.consumer.ID()]
			version := consumer.version
			if version == 0 {
				version = c.id.Version
			}
			if err := ret.tryAssign(info, version); err != nil {
				log.Debugf(ctx, "Old grant is no longer valid in a new cluster map. Discarding. Grant: %v. Reason: %v", info.grant, err)
				continue // skip: invalid grant
			}
		}

		return ret, nil

	case msg.IsChange():
		change, _ := msg.Change()

		id := msg.ID()

		if !c.ID().IsNext(id, msg.Version()) {
			return nil, fmt.Errorf("unexpected incremental update for %v: %v v%v", c.ID(), id, msg.Version())
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

		assignments := change.Assign().Assignments()
		updated := mapx.New(change.Update().Grants(), GrantInfo.ID)
		unassigned := slicex.NewSet(change.Unassign().Grants()...)
		removed := slicex.NewSet(change.Remove().Consumers()...)

		if err := c.validateChange(shards, assignments, updated, unassigned, removed); err != nil {
			return nil, fmt.Errorf("invalid cluster change: %v", err)
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

		// Add new assignments

		ret.initAssignments(assignments)

		// Copy over updated grants

		for _, g := range change.Update().Grants() {
			if old, ok := c.grants[g.ID()]; ok {
				old.grant = g
				old.version = 0
				ret.assign(old, 0)
			}
		}

		// Copy over retained values

		for cid, info := range c.consumers {
			if removed[cid] {
				continue // skip: consumer removed
			}

			// Ensure the consumer is copied even if it has no grants. Note that snapshot will remove old consumers
			// without grants.
			if _, ok := ret.consumers[cid]; !ok {
				ret.consumers[cid] = consumerInfo{consumer: info.consumer, grants: make([]GrantInfo, 0, len(info.grants)), version: info.version}
			}

			for _, g := range info.grants {
				if _, ok := ret.grants[g.ID()]; ok || unassigned[g.ID()] {
					continue // skip: grant assigned, updated or removed
				}

				old := c.grants[g.ID()]
				if err := ret.tryAssign(old, info.version); err != nil {
					log.Debugf(ctx, "Old grant is no longer valid in a new cluster map. Discarding. Grant: %v. Reason: %v", old.grant, err)
					continue // skip: invalid grant
				}
			}
		}

		return ret, nil

	default:
		return nil, fmt.Errorf("unexpected message type: %v", msg)
	}
}

// tryAssign adds a grant to the cluster if it's valid and not conflicting with existing grants.
func (c *ClusterMap) tryAssign(info grantInfo, consumerVersion int) error {
	g := info.grant

	shardGrants, ok := c.shards[g.Shard()]
	if !ok {
		return fmt.Errorf("unknown shard")
	}

	// Grant should not be present in shard grants (per the first check). Check other grants for the same shard.
	if err := grantCanBeAssignedToShard(shardGrants, func(id GrantID) GrantInfo { return c.grants[id].grant }, g); err != nil {
		return err
	}

	c.assign(info, consumerVersion)
	return nil
}

// grantStatesCompatible verifies that states of the given grant states are compatible for the same shard.
func grantStatesCompatible(state1 GrantState, state2 GrantState) bool {
	allocated1 := IsAllocatedOrLoaded(state1)
	revoked1 := IsRevokedOrUnloaded(state1)
	allocated2 := IsAllocatedOrLoaded(state2)
	revoked2 := IsRevokedOrUnloaded(state2)

	return (allocated1 && revoked2) || (allocated2 && revoked1)
}

func (c *ClusterMap) initAssignments(assignments []Assignment) {
	for _, a := range assignments {
		grants := a.Grants()
		consumer := a.Consumer()

		c.consumers[consumer.ID()] = consumerInfo{consumer: consumer, grants: make([]GrantInfo, 0, len(grants))}
		for _, g := range grants {
			info := grantInfo{consumer: consumer, grant: g}
			c.assign(info, 0)
		}
	}
}

func (c *ClusterMap) ensureShard(shard Shard) map[GrantID]bool {
	if _, ok := c.shards[shard]; !ok {
		c.shards[shard] = map[GrantID]bool{}
	}
	return c.shards[shard]
}

// assign adds a new grant to the cluster. Doesn't perform validation.
func (c *ClusterMap) assign(grant grantInfo, consumerVersion int) {
	gid := grant.grant.ID()
	shard := grant.grant.Shard()
	c.ensureShard(shard)[gid] = true

	info, ok := c.consumers[grant.consumer.ID()]
	if !ok {
		info = consumerInfo{consumer: grant.consumer, grants: []GrantInfo{}, version: consumerVersion}
	}
	info.grants = append(info.grants, grant.grant)
	c.consumers[grant.consumer.ID()] = info
	grant.consumer = info.consumer

	c.grants[gid] = grant
	c.cache.Write(shard, gid, grant)
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

func (c *ClusterMap) validateSnapshot(shards []Shard, assignments []Assignment) error {
	shardGrants, err := c.validateShards(shards)
	if err != nil {
		return err
	}
	return c.validateAssignments(shardGrants, map[GrantID]GrantInfo{}, assignments)
}

// validateChange checks if the change is valid and can be applied to the current cluster state. It assumes that
// changes are only for grants with valid shards; grants for invalid shards are deleted implicitly.
func (c *ClusterMap) validateChange(shards []Shard, assignments []Assignment, updated map[GrantID]GrantInfo, unassigned map[GrantID]bool, removed map[ConsumerID]bool) error {
	shardGrants, err := c.validateShards(shards)
	if err != nil {
		return err
	}

	// Copy existing grants for valid shards
	totalGrants := map[GrantID]GrantInfo{}
	for _, info := range c.grants {
		if _, ok := shardGrants[info.grant.Shard()]; !ok { // invalid shard, ignore grant
			continue
		}
		if info.Retained() { // Ignore grants from cluster map before the most recent snapshot
			continue
		}
		totalGrants[info.grant.ID()] = info.grant
	}

	// Updates should be sent for registered grants only
	for gid, info := range updated {
		old, ok := totalGrants[gid]
		if !ok {
			return fmt.Errorf("updated unregistered grant %v", gid)
		}
		if err := validateGrantUpdate(old, info); err != nil {
			return fmt.Errorf("invalid update for grant %v: %v", gid, err)
		}
		totalGrants[gid] = info
	}

	// Only registered grants should be unassigned
	for gid := range unassigned {
		if _, ok := totalGrants[gid]; !ok {
			return fmt.Errorf("unassigned unregistered grant %v", gid)
		}
		delete(totalGrants, gid)
	}

	// Only registered consumers should be removed. Remove their grants too.
	for cid := range removed {
		info, ok := c.consumers[cid]
		if !ok || info.Retained() {
			return fmt.Errorf("removed unregistered consumer %v", cid)
		}
		for _, g := range info.grants {
			delete(totalGrants, g.ID())
		}
	}

	// Register old valid grants
	for _, info := range totalGrants {
		shardGrants[info.Shard()][info.ID()] = true
	}

	// Pass registered grants, but not consumers. New assignments cannot contain registered grants, but can contain
	// registered consumers (with new, additional grants).
	return c.validateAssignments(shardGrants, totalGrants, assignments)
}

// validateShards checks if the shards are unique and returns a map of shards to grants.
func (c *ClusterMap) validateShards(shards []Shard) (map[Shard]map[GrantID]bool, error) {
	shardGrants := map[Shard]map[GrantID]bool{}
	for _, shard := range shards {
		if _, ok := shardGrants[shard]; ok {
			return nil, fmt.Errorf("duplicate shard %v", shard)
		}
		shardGrants[shard] = map[GrantID]bool{}
	}
	return shardGrants, nil
}

// validateAssignments checks if the assignments are valid and don't conflict with passed grants or grants
// in other assignments. It updates given maps with the new grants.
func (c *ClusterMap) validateAssignments(shards map[Shard]map[GrantID]bool, grants map[GrantID]GrantInfo, assignments []Assignment) error {
	consumers := map[ConsumerID]bool{}
	for _, assignment := range assignments {
		cid := assignment.Consumer().ID()

		// Consumer can only be listed once
		if _, ok := consumers[cid]; ok {
			return fmt.Errorf("duplicate consumer %v", cid)
		}
		consumers[cid] = true

		// Validate consumer grants
		for _, grant := range assignment.Grants() {
			if err := c.validateGrant(shards, grants, grant); err != nil {
				return fmt.Errorf("consumer %v has invalid grant %v in assignments: %v", cid, grant.ID(), err)
			}
			shards[grant.Shard()][grant.ID()] = true
			grants[grant.ID()] = grant
		}
	}
	return nil
}

// validateGrant checks if the grant is valid and doesn't conflict with other grants assigned to the same shard.
func (c *ClusterMap) validateGrant(shards map[Shard]map[GrantID]bool, grants map[GrantID]GrantInfo, grant GrantInfo) error {
	gid := grant.ID()
	shard := grant.Shard()
	// Grant can only be assigned once
	if _, ok := grants[gid]; ok {
		return fmt.Errorf("duplicate grant")
	}

	// Shard should be valid
	shardGrants, ok := shards[shard]
	if !ok {
		return fmt.Errorf("unknown shard %v", shard)
	}

	return grantCanBeAssignedToShard(shardGrants, func(id GrantID) GrantInfo { return grants[id] }, grant)
}

// grantCanBeAssignedToShard checks whether a grant can be assigned to a shard. A grant can be assigned
// to a shard if it's not conflicting with other grants assigned to the shard.
func grantCanBeAssignedToShard(shardGrants map[GrantID]bool, grants func(GrantID) GrantInfo, grant GrantInfo) error {
	switch len(shardGrants) {
	case 0:
	case 1:
		otherGrantID, _, _ := mapx.GetOnly(shardGrants)
		otherGrant := grants(otherGrantID)
		if !grantStatesCompatible(grant.State(), otherGrant.State()) {
			return fmt.Errorf("grant %v has conflicting state with another grant assigned to the same shard: %v", grant, otherGrant)
		}
	default:
		ids := append(mapx.Keys(shardGrants), grant.ID())
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		return fmt.Errorf("shard %v has too many assigned grants: %v", grant.Shard(), ids)
	}
	return nil
}

func validateGrantUpdate(old GrantInfo, new GrantInfo) error {
	if old.Shard() != new.Shard() {
		return fmt.Errorf("shard mismatch: %v != %v", old.Shard(), new.Shard())
	}
	if !GrantStateCanAdvanceTo(old.State(), new.State()) {
		return fmt.Errorf("state did not advance: %v >= %v", old.State(), new.State())
	}
	return nil
}

// GrantRetainedVersion returns version of the cluster the grant was retained from. For testing only.
func GrantRetainedVersion(c *ClusterMap, gid GrantID) (int, bool) {
	if info, ok := c.grants[gid]; ok {
		return info.version, true
	}
	return 0, false
}

// ConsumerRetainedVersion returns version of the cluster the consumer was retained from. For testing only.
func ConsumerRetainedVersion(c *ClusterMap, cid ConsumerID) (int, bool) {
	if info, ok := c.consumers[cid]; ok {
		return info.version, true
	}
	return 0, false
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

// DomainShards returns a list of shards in the cluster for the given domain
func DomainShards(cluster Cluster, domain QualifiedDomainName) []Shard {
	return slicex.Filter(cluster.Shards(), func(s Shard) bool { return s.Domain == domain })
}
