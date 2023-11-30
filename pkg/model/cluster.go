package model

import (
	"context"
	"go.atoms.co/lib/mapx"
	"go.atoms.co/slicex"
	"fmt"
	"github.com/google/uuid"
	"sync"
)

type ClusterId string

func NewClusterID() ClusterId {
	return ClusterId(uuid.New().String())
}

// Cluster contains information about all consumers included in the work distribution process.
type Cluster interface {
	// Owner finds the owner of active or revoked shard that contains the provided key.
	Owner(key QualifiedDomainKey) (Instance, GrantState, bool)

	// OwnerWithState finds the owner of a shard in the given state that contains the provided key
	OwnerWithState(key QualifiedDomainKey, state GrantState) (Instance, bool)

	// Consumer returns consumer information by its id
	Consumer(id ConsumerID) (Consumer, bool)

	// Consumers returns all the consumers joined the work distribution process (even without any assigned shards)
	Consumers() []Consumer

	// Grant returns grant information of the given grant id.
	Grant(gid GrantID) (GrantInfo, bool)

	// Grants returns grants assigned to the provided consumer.
	Grants(cid ConsumerID) []GrantInfo

	// GrantConsumer returns a consumer of the given grant if it exists.
	GrantConsumer(gid GrantID) (Consumer, bool)

	// ShardGrants returns grants assigned to the given shard
	ShardGrants(shard Shard) []GrantID

	// Update modifies the cluster: updates the list of consumers, assigns, updates and removes grants.
	Update(consumers []Consumer, assigned map[ConsumerID][]GrantInfo, updated []GrantInfo, unassigned []GrantID, detached []ConsumerID) error

	// Diff calculates the difference between two clusters
	Diff(old Cluster) ClusterDiff

	ID() ClusterId

	Version() int
}

// ClusterDiff contains changes in a cluster map
type ClusterDiff struct {
	Assigned   map[ConsumerID][]GrantInfo
	Updated    []GrantInfo
	Unassigned []GrantID
	Detached   []ConsumerID
}

func (d ClusterDiff) IsEmpty() bool {
	return len(d.Assigned) == 0 && len(d.Updated) == 0 && len(d.Unassigned) == 0 && len(d.Detached) == 0
}

type cluster struct {
	id      ClusterId
	version int

	consumers map[ConsumerID]Consumer
	grants    map[GrantID]GrantInfo

	c2g map[ConsumerID]map[GrantID]bool
	g2c map[GrantID]ConsumerID
	d2g map[QualifiedDomainName]map[GrantID]bool
	s2g map[Shard]map[GrantID]bool
}

func NewCluster(id ClusterId, version int, consumers []Consumer, grants map[ConsumerID][]GrantInfo) (Cluster, error) {
	c := &cluster{
		id:        id,
		version:   version,
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

func (c *cluster) ID() ClusterId {
	return c.id
}

func (c *cluster) Version() int {
	return c.version
}

func (c *cluster) String() string {
	return fmt.Sprintf("cluster{id=%v/%v, #consumers=%v, #grants=%v}", c.id, c.version, len(c.consumers), len(c.grants))
}

func (c *cluster) Owner(key QualifiedDomainKey) (Instance, GrantState, bool) {
	if grants, ok := c.d2g[key.Domain]; ok {
		for gid := range grants {
			grant := c.grants[gid]
			if grant.Shard.Contains(key) && (IsActiveGrant(grant.State) || IsRevokedGrant(grant.State)) {
				return c.consumers[c.g2c[gid]], grant.State, true
			}
		}
	}
	return Instance{}, InvalidGrant, false
}

func (c *cluster) OwnerWithState(key QualifiedDomainKey, state GrantState) (Instance, bool) {
	if grants, ok := c.d2g[key.Domain]; ok {
		for gid := range grants {
			grant := c.grants[gid]
			if grant.Shard.Contains(key) && grant.State == state {
				return c.consumers[c.g2c[gid]], true
			}
		}
	}
	return Instance{}, false
}

func (c *cluster) Consumer(id ConsumerID) (Consumer, bool) {
	if consumer, ok := c.consumers[id]; ok {
		return consumer, true
	}
	return Consumer{}, false
}

func (c *cluster) Consumers() []Consumer {
	return mapx.Values(c.consumers)
}

func (c *cluster) Grant(gid GrantID) (GrantInfo, bool) {
	g, ok := c.grants[gid]
	return g, ok
}

func (c *cluster) Grants(cid ConsumerID) []GrantInfo {
	return slicex.Map(mapx.Keys(c.c2g[cid]), func(gid GrantID) GrantInfo { return c.grants[gid] })
}

func (c *cluster) ShardGrants(shard Shard) []GrantID {
	return mapx.Keys(c.s2g[shard])
}

func (c *cluster) GrantConsumer(gid GrantID) (Consumer, bool) {
	if cid, ok := c.g2c[gid]; ok {
		consumer, ok := c.consumers[cid]
		return consumer, ok
	}
	return Consumer{}, false
}

func (c *cluster) Update(consumers []Consumer, assigned map[ConsumerID][]GrantInfo, updated []GrantInfo, unassigned []GrantID, detached []ConsumerID) error {
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
	err = c.detach(detached...)
	if err != nil {
		return err
	}
	c.version++
	return nil
}

func (c *cluster) assign(consumers []Consumer, grants map[ConsumerID][]GrantInfo) error {
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

			// Check shard moved to another consumer
			if gid, ok := c.findGrant(g.Shard, g.State); ok && c.g2c[gid] != cid {
				c.deleteGrant(gid)
			}

			c.grants[g.ID] = g
			c.c2g[cid][g.ID] = true
			c.g2c[g.ID] = cid
			c.ensureShard(g.Shard)
			c.s2g[g.Shard][g.ID] = true

			// Not checking for changes in shard domain, not supported
			c.ensureDomain(g.Shard.Domain)
			c.d2g[g.Shard.Domain][g.ID] = true
		}
	}
	return nil
}

func (c *cluster) update(grants ...GrantInfo) error {
	for _, g := range grants {
		if _, ok := c.grants[g.ID]; !ok {
			return fmt.Errorf("unknown grant: %v", g)
		}
	}
	for _, g := range grants {
		c.activateGrantIfNeeded(g)
		c.grants[g.ID] = g
	}
	return nil
}

func (c *cluster) unassign(grants ...GrantID) error {
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

func (c *cluster) detach(consumers ...ConsumerID) error {
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

func (c *cluster) Diff(old Cluster) ClusterDiff {
	var detached []ConsumerID
	oldGrants := map[GrantID]GrantInfo{}
	for _, consumer := range old.Consumers() {
		if _, ok := c.Consumer(consumer.ID()); !ok {
			detached = append(detached, consumer.ID())
		}
		for _, g := range old.Grants(consumer.ID()) {
			oldGrants[g.ID] = g
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
		for _, gid := range old.ShardGrants(newGrant.Shard) {
			g, _ := old.Grant(gid)
			if g.State == newGrant.State {
				toDelete = append(toDelete, gid)
			}
		}

		// When a new allocated grant is created, another active grant for the same shard is revoked implicitly
		if IsAllocatedGrant(newGrant.State) {
			for gid := range c.s2g[newGrant.Shard] {
				if IsActiveOrRevokedGrant(c.grants[gid].State) {
					// Remove potential update (or no update)
					if oldGrant, ok := old.Grant(gid); ok && IsActiveOrRevokedGrant(oldGrant.State) {
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
		if oldGrant == newGrant {
			continue
		}

		updated = append(updated, newGrant)

		// On a transition from allocated to active, the revoked grant is removed implicitly
		if IsAllocatedGrant(oldGrant.State) && IsActiveGrant(newGrant.State) {
			// Removal of revoked is implicit
			for _, gid := range old.ShardGrants(oldGrant.Shard) {
				if grant, _ := old.Grant(gid); IsRevokedGrant(grant.State) {
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
		Detached:   detached,
	}
}

func (c *cluster) findGrant(s Shard, state GrantState) (GrantID, bool) {
	if grants, ok := c.s2g[s]; ok {
		for id := range grants {
			if c.grants[id].State == state {
				return id, true
			}
		}
	}
	return "", false
}

func (c *cluster) ensureConsumer(cid ConsumerID) {
	if _, ok := c.c2g[cid]; !ok {
		c.c2g[cid] = map[GrantID]bool{}
	}
}

func (c *cluster) ensureShard(s Shard) {
	if _, ok := c.s2g[s]; !ok {
		c.s2g[s] = map[GrantID]bool{}
	}
}

func (c *cluster) ensureDomain(domain QualifiedDomainName) {
	if _, ok := c.d2g[domain]; !ok {
		c.d2g[domain] = map[GrantID]bool{}
	}
}

func (c *cluster) activateGrantIfNeeded(g GrantInfo) {
	// On transition from allocated to active, remove the revoked grant for the same shard, but different ID
	if IsActiveGrant(g.State) {
		if old, ok := c.grants[g.ID]; ok && IsAllocatedGrant(old.State) {
			c.deleteRevokedGrant(g.Shard)
		}
	}
}

func (c *cluster) deleteRevokedGrant(shard Shard) {
	for gid := range c.s2g[shard] {
		grant := c.grants[gid]
		if IsRevokedGrant(grant.State) {
			c.deleteGrant(gid)
			return
		}
	}
}

func (c *cluster) deleteGrant(g GrantID) {
	grant := c.grants[g]
	consumer := c.g2c[g]
	delete(c.grants, g)
	delete(c.c2g[consumer], g)
	delete(c.g2c, g)
	delete(c.d2g[grant.Shard.Domain], g)
	delete(c.s2g[grant.Shard], g)
}

type ClusterProvider interface {
	Cluster() (Cluster, bool)
}

func NewClusterProvider(ctx context.Context, clusters <-chan Cluster) ClusterProvider {
	p := clusterProvider{}
	go p.process(ctx, clusters)
	return &p
}

type clusterProvider struct {
	cluster Cluster
	mu      sync.RWMutex
}

func (p *clusterProvider) Cluster() (Cluster, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.cluster, p.cluster != nil
}

func (p *clusterProvider) process(ctx context.Context, clusters <-chan Cluster) {
	for {
		select {
		case c, ok := <-clusters:
			if !ok {
				return
			}
			p.mu.Lock()
			p.cluster = c
			p.mu.Unlock()
		case <-ctx.Done():
			return
		}
	}
}
