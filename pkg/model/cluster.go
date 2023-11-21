package model

import (
	"context"
	"go.atoms.co/lib/mapx"
	"go.atoms.co/slicex"
	"fmt"
	"sync"
)

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

	// Assign updates the list of consumers and consumers grants (incrementally).
	Assign(consumers []Consumer, grants map[ConsumerID][]GrantInfo) error

	// Update modifies the state of grants without changing their ownership.
	Update(grants ...GrantInfo) error

	// Unassign marks given grants as unassigned to consumers.
	Unassign(grants ...GrantID) error

	// Detach removes consumers from the cluster together with grants assigned to them (unassigning grants).
	Detach(consumers ...ConsumerID) error
}

type cluster struct {
	consumers map[ConsumerID]Consumer
	grants    map[GrantID]GrantInfo

	c2g map[ConsumerID]map[GrantID]bool
	g2c map[GrantID]ConsumerID
	d2g map[QualifiedDomainName]map[GrantID]bool
}

func NewCluster(consumers []Consumer, grants map[ConsumerID][]GrantInfo) (Cluster, error) {
	c := &cluster{
		consumers: map[ConsumerID]Consumer{},
		grants:    map[GrantID]GrantInfo{},
		c2g:       map[ConsumerID]map[GrantID]bool{},
		g2c:       map[GrantID]ConsumerID{},
		d2g:       map[QualifiedDomainName]map[GrantID]bool{},
	}
	err := c.Assign(consumers, grants)
	if err != nil {
		return nil, err
	}
	return c, nil
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

func (c *cluster) GrantConsumer(gid GrantID) (Consumer, bool) {
	if cid, ok := c.g2c[gid]; ok {
		consumer, ok := c.consumers[cid]
		return consumer, ok
	}
	return Consumer{}, false
}

func (c *cluster) Assign(consumers []Consumer, grants map[ConsumerID][]GrantInfo) error {
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

			// Check grant moved to another consumer
			if old, ok := c.g2c[g.ID]; ok && old != cid {
				delete(c.c2g[old], g.ID)
			}

			c.grants[g.ID] = g
			c.c2g[cid][g.ID] = true
			c.g2c[g.ID] = cid

			// Not checking for changes in shard domain, not supported
			c.ensureDomain(g.Shard.Domain)
			c.d2g[g.Shard.Domain][g.ID] = true
		}
	}
	return nil
}

func (c *cluster) Update(grants ...GrantInfo) error {
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

func (c *cluster) Unassign(grants ...GrantID) error {
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

func (c *cluster) Detach(consumers ...ConsumerID) error {
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

func (c *cluster) ensureConsumer(cid ConsumerID) {
	if _, ok := c.c2g[cid]; !ok {
		c.c2g[cid] = map[GrantID]bool{}
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
	for gid := range c.d2g[shard.Domain] {
		grant := c.grants[gid]
		if grant.Shard == shard && IsRevokedGrant(grant.State) {
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
