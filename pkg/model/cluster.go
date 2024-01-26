package model

import (
	"context"
	"go.atoms.co/splitter/lib/service/location"
	"go.atoms.co/lib/chanx"
	"go.atoms.co/lib/mapx"
	"go.atoms.co/slicex"
	"fmt"
	"time"
)

// ClusterID identifies cluster version and origin information.
type ClusterID struct {
	Origin    location.Instance
	Version   int
	Timestamp time.Time
}

func (c ClusterID) String() string {
	return fmt.Sprintf("%v[origin=%v, updated=%v]", c.Version, c.Origin, c.Timestamp)
}

// Cluster contains information about all consumers and grants in the work distribution process.
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

// ClusterDiff contains changes in a cluster map
type ClusterDiff struct {
	Assigned   map[ConsumerID][]GrantInfo
	Updated    []GrantInfo
	Unassigned []GrantID
	Removed    []ConsumerID
}

func (d ClusterDiff) IsEmpty() bool {
	return len(d.Assigned) == 0 && len(d.Updated) == 0 && len(d.Unassigned) == 0 && len(d.Removed) == 0
}

type ClusterMap struct {
	id ClusterID

	consumers map[ConsumerID]Consumer
	grants    map[GrantID]GrantInfo

	c2g map[ConsumerID]map[GrantID]bool
	g2c map[GrantID]ConsumerID
	d2g map[QualifiedDomainName]map[GrantID]bool
	s2g map[Shard]map[GrantID]bool
}

func NewCluster(id ClusterID, consumers []Consumer, grants map[ConsumerID][]GrantInfo) (*ClusterMap, error) {
	c := &ClusterMap{
		id:        id,
		consumers: map[ConsumerID]Consumer{},
		grants:    map[GrantID]GrantInfo{},
		c2g:       map[ConsumerID]map[GrantID]bool{},
		g2c:       map[GrantID]ConsumerID{},
		d2g:       map[QualifiedDomainName]map[GrantID]bool{},
		s2g:       map[Shard]map[GrantID]bool{},
	}
	err := c.assign(consumers, grants)
	if err != nil {
		return nil, err
	}
	return c, nil
}

func (c *ClusterMap) ID() ClusterID {
	return c.id
}

func (c *ClusterMap) Clone() *ClusterMap {
	ret, _ := NewCluster(c.ID(), c.Consumers(), mapx.Map(c.consumers, func(k ConsumerID, v Consumer) (ConsumerID, []GrantInfo) {
		return k, c.Grants(k)
	}))
	return ret
}

func (c *ClusterMap) String() string {
	return fmt.Sprintf("cluster{id=%v, #consumers=%v, #grants=%v}", c.id, len(c.consumers), len(c.grants))
}

func (c *ClusterMap) Lookup(key QualifiedDomainKey, states ...GrantState) (Instance, GrantInfo, bool) {
	if len(states) == 0 {
		states = []GrantState{ActiveGrantState, RevokedGrantState}
	}

	if grants, ok := c.d2g[key.Domain]; ok {
		candidates := map[GrantState]GrantInfo{}
		for gid := range grants {
			if g, ok := c.grants[gid]; ok && g.Shard().Contains(key) {
				candidates[g.State()] = g
			}
		}

		for _, state := range states {
			if info, ok := candidates[state]; ok {
				return c.consumers[c.g2c[info.ID()]], info, true
			}
		} // else: no grant in requested state

	}
	return Instance{}, GrantInfo{}, false
}

func (c *ClusterMap) Owner(key QualifiedDomainKey) (Instance, GrantState, bool) {
	if grants, ok := c.d2g[key.Domain]; ok {
		for gid := range grants {
			grant := c.grants[gid]
			if grant.Shard().Contains(key) && (IsActiveGrant(grant.State()) || IsRevokedGrant(grant.State())) {
				return c.consumers[c.g2c[gid]], grant.State(), true
			}
		}
	}
	return Instance{}, InvalidGrantState, false
}

func (c *ClusterMap) OwnerWithState(key QualifiedDomainKey, state GrantState) (Instance, bool) {
	if grants, ok := c.d2g[key.Domain]; ok {
		for gid := range grants {
			grant := c.grants[gid]
			if grant.Shard().Contains(key) && grant.State() == state {
				return c.consumers[c.g2c[gid]], true
			}
		}
	}
	return Instance{}, false
}

func (c *ClusterMap) Consumers() []Consumer {
	return mapx.Values(c.consumers)
}

func (c *ClusterMap) Consumer(id ConsumerID) (Consumer, []GrantInfo, bool) {
	if consumer, ok := c.consumers[id]; ok {
		return consumer, c.Grants(id), true
	}
	return Consumer{}, nil, false
}

func (c *ClusterMap) Grant(gid GrantID) (Consumer, GrantInfo, bool) {
	if g, ok := c.grants[gid]; ok {
		return c.consumers[c.g2c[gid]], g, true
	}
	return Consumer{}, GrantInfo{}, false
}

func (c *ClusterMap) Grants(cid ConsumerID) []GrantInfo {
	return slicex.Map(mapx.Keys(c.c2g[cid]), func(gid GrantID) GrantInfo { return c.grants[gid] })
}

func (c *ClusterMap) ShardGrants(shard Shard) []GrantID {
	return mapx.Keys(c.s2g[shard])
}

func (c *ClusterMap) GrantConsumer(gid GrantID) (Consumer, bool) {
	if cid, ok := c.g2c[gid]; ok {
		consumer, ok := c.consumers[cid]
		return consumer, ok
	}
	return Consumer{}, false
}

func (c *ClusterMap) Update(consumers []Consumer, assigned map[ConsumerID][]GrantInfo, updated []GrantInfo, unassigned []GrantID, removed []ConsumerID, now time.Time) error {
	err := c.assign(consumers, assigned)
	if err != nil {
		return err
	}
	err = c.update(updated...)
	if err != nil {
		return err
	}
	err = c.unassign(unassigned...)
	if err != nil {
		return err
	}
	err = c.remove(removed...)
	if err != nil {
		return err
	}
	c.id.Version++
	c.id.Timestamp = now
	return nil
}

func (c *ClusterMap) assign(consumers []Consumer, grants map[ConsumerID][]GrantInfo) error {
	ids := mapx.MapNew(consumers, func(c Consumer) (ConsumerID, bool) {
		return c.ID(), true
	})

	// Verify integrity before mutating data
	for cid := range grants {
		if _, ok := ids[cid]; !ok {
			return fmt.Errorf("consumer information is missing in assignments: %v", cid)
		}
	}

	for _, consumer := range consumers {
		// Store consumer information on first assignment
		if _, ok := c.consumers[consumer.ID()]; !ok {
			c.consumers[consumer.ID()] = consumer
			c.ensureConsumer(consumer.ID())
		}
	}

	for cid, consumerGrants := range grants {
		for _, g := range consumerGrants {
			c.activateGrantIfNeeded(g)
			c.revokeGrantIfNeeded(g)

			// Check shard moved to another consumer
			if gid, ok := c.findGrant(g.Shard(), g.State()); ok && c.g2c[gid] != cid {
				c.deleteGrant(gid)
			}

			c.grants[g.ID()] = g
			c.c2g[cid][g.ID()] = true
			c.g2c[g.ID()] = cid
			c.ensureShard(g.Shard())
			c.s2g[g.Shard()][g.ID()] = true

			// Not checking for changes in shard domain, not supported
			c.ensureDomain(g.Shard().Domain)
			c.d2g[g.Shard().Domain][g.ID()] = true
		}
	}
	return nil
}

func (c *ClusterMap) update(grants ...GrantInfo) error {
	for _, g := range grants {
		if _, ok := c.grants[g.ID()]; !ok {
			return fmt.Errorf("unknown grant: %v", g)
		}
	}
	for _, g := range grants {
		c.activateGrantIfNeeded(g)
		c.grants[g.ID()] = g
	}
	return nil
}

func (c *ClusterMap) unassign(grants ...GrantID) error {
	for _, g := range grants {
		if _, ok := c.grants[g]; !ok {
			return fmt.Errorf("unknown grant: %v", g)
		}
	}
	for _, g := range grants {
		c.deleteGrant(g)
	}
	return nil
}

func (c *ClusterMap) remove(consumers ...ConsumerID) error {
	for _, consumer := range consumers {
		if _, ok := c.consumers[consumer]; !ok {
			return fmt.Errorf("unknown consumer: %v", consumer)
		}
	}
	for _, consumer := range consumers {
		delete(c.consumers, consumer)
		for g := range c.c2g[consumer] {
			c.deleteGrant(g)
		}
		delete(c.c2g, consumer)
	}
	return nil
}

func (c *ClusterMap) Diff(old *ClusterMap) ClusterDiff {
	var removed []ConsumerID
	oldGrants := map[GrantID]GrantInfo{}
	for _, consumer := range old.Consumers() {
		if _, ok := c.consumers[consumer.ID()]; !ok {
			removed = append(removed, consumer.ID())
		}
		for _, g := range old.Grants(consumer.ID()) {
			oldGrants[g.ID()] = g
		}
	}

	assigned := map[ConsumerID][]GrantInfo{}
	var toDelete []GrantID

	// Process new grants only
	for newGid, newGrant := range c.grants {
		if _, ok := oldGrants[newGid]; ok {
			continue
		}

		assigned[c.g2c[newGid]] = append(assigned[c.g2c[newGid]], newGrant)

		// Old grant implicitly removed when shard is assigned with a new grant ID
		// This optimization avoids surfacing implicit revokes in the Diff
		for _, gid := range old.ShardGrants(newGrant.Shard()) {
			_, g, _ := old.Grant(gid)
			if g.State() == newGrant.State() {
				toDelete = append(toDelete, gid)
			}
		}

		// When a new allocated grant is created, another active grant for the same shard is revoked implicitly
		if IsAllocatedGrant(newGrant.State()) {
			for gid := range c.s2g[newGrant.Shard()] {
				if IsActiveOrRevokedGrant(c.grants[gid].State()) {
					// Remove potential update (or no update)
					if _, oldGrant, ok := old.Grant(gid); ok && IsActiveOrRevokedGrant(oldGrant.State()) {
						toDelete = append(toDelete, gid)
					}
				}
			}
		}
	}

	for _, gid := range toDelete {
		delete(oldGrants, gid)
	}

	var updated []GrantInfo

	// Process updated grants
	for newGid, newGrant := range c.grants {
		oldGrant, ok := oldGrants[newGid]
		if !ok {
			continue
		}
		if oldGrant.Equals(newGrant) {
			continue
		}
		updated = append(updated, newGrant)

		// On a transition from allocated to active, the revoked grant is removed implicitly
		if IsAllocatedGrant(oldGrant.State()) && IsActiveGrant(newGrant.State()) {
			// Removal of revoked is implicit
			for _, gid := range old.ShardGrants(oldGrant.Shard()) {
				if _, grant, _ := old.Grant(gid); IsRevokedGrant(grant.State()) {
					delete(oldGrants, gid)
				}
			}
		}
	}

	var unassigned []GrantID

	for gid := range oldGrants {
		if _, ok := c.grants[gid]; !ok {
			unassigned = append(unassigned, gid)
		}
	}

	return ClusterDiff{
		Assigned:   assigned,
		Updated:    updated,
		Unassigned: unassigned,
		Removed:    removed,
	}
}

func (c *ClusterMap) findGrant(s Shard, state GrantState) (GrantID, bool) {
	if grants, ok := c.s2g[s]; ok {
		for id := range grants {
			if c.grants[id].State() == state {
				return id, true
			}
		}
	}
	return "", false
}

func (c *ClusterMap) ensureConsumer(cid ConsumerID) {
	if _, ok := c.c2g[cid]; !ok {
		c.c2g[cid] = map[GrantID]bool{}
	}
}

func (c *ClusterMap) ensureShard(s Shard) {
	if _, ok := c.s2g[s]; !ok {
		c.s2g[s] = map[GrantID]bool{}
	}
}

func (c *ClusterMap) ensureDomain(domain QualifiedDomainName) {
	if _, ok := c.d2g[domain]; !ok {
		c.d2g[domain] = map[GrantID]bool{}
	}
}

func (c *ClusterMap) activateGrantIfNeeded(g GrantInfo) {
	// On transition from allocated to active, remove the revoked grant for the same shard, but different ID
	if IsActiveGrant(g.State()) {
		if old, ok := c.grants[g.ID()]; ok && IsAllocatedGrant(old.State()) {
			c.deleteRevokedGrant(g.Shard())
		}
	}
}

func (c *ClusterMap) revokeGrantIfNeeded(g GrantInfo) {
	// On Allocation, previous Active grant should be transitioned to Revoked state
	if IsAllocatedGrant(g.State()) {
		if gid, ok := c.findGrant(g.Shard(), ActiveGrantState); ok {
			c.grants[gid] = NewGrantInfo(gid, c.grants[gid].Shard(), RevokedGrantState)
		}
	}
}

func (c *ClusterMap) deleteRevokedGrant(shard Shard) {
	for gid := range c.s2g[shard] {
		grant := c.grants[gid]
		if IsRevokedGrant(grant.State()) {
			c.deleteGrant(gid)
			return
		}
	}
}

func (c *ClusterMap) deleteGrant(g GrantID) {
	grant := c.grants[g]
	consumer := c.g2c[g]
	delete(c.grants, g)
	delete(c.c2g[consumer], g)
	delete(c.g2c, g)
	delete(c.d2g[grant.Shard().Domain], g)
	delete(c.s2g[grant.Shard()], g)
}

// ClusterProvider returns the latest Cluster, if one is present. Thread-safe.
type ClusterProvider interface {
	V() (Cluster, bool)
}

func NewClusterProvider(ctx context.Context, clusters <-chan Cluster) ClusterProvider {
	return chanx.NewProvider(ctx, clusters)
}
