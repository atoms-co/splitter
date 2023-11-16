package model

import (
	"context"
	"fmt"
	"sync"
)

// Cluster contains information about all consumers included in the work distribution process.
type Cluster interface {
	// Owner finds the owner of a shard that contains the provided key
	Owner(key QualifiedDomainKey) (Instance, bool)

	// Consumer returns consumer information by its id
	Consumer(id ConsumerID) (Consumer, bool)

	// Consumers returns all the consumers joined the work distribution process (even without any assigned shards)
	Consumers() []Consumer

	// Assign updates the list of consumers and consumers grants (incrementally).
	Assign(consumers []Consumer, grants map[ConsumerID][]GrantInfo) error

	// Update modifies the state of grants without changing their ownership.
	Update(grants []GrantInfo) error

	// Unassign marks given grants as unassigned to consumers.
	Unassign(grants []GrantID) error

	// Detach removes consumers from the cluster together with grants assigned to them (unassigning grants).
	Detach(consumers []ConsumerID) error
}

type cluster struct {
}

func (c *cluster) Owner(key QualifiedDomainKey) (Instance, bool) {
	return Instance{}, false
}

func (c *cluster) Consumer(id ConsumerID) (Consumer, bool) {
	return Consumer{}, false
}

func (c *cluster) Consumers() []Consumer {
	return nil
}

func (c *cluster) Assign(consumers []Consumer, grants map[ConsumerID][]GrantInfo) error {
	return fmt.Errorf("not implemented")
}

func (c *cluster) Update(grants []GrantInfo) error {
	return fmt.Errorf("not implemented")
}

func (c *cluster) Unassign(grants []GrantID) error {
	return fmt.Errorf("not implemented")
}

func (c *cluster) Detach(consumers []ConsumerID) error {
	return fmt.Errorf("not implemented")
}

func NewCluster(consumers []Consumer, grants map[ConsumerID][]GrantInfo) Cluster {
	return &cluster{}
}

type ClusterProvider interface {
	Cluster() Cluster
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

func (p *clusterProvider) Cluster() Cluster {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.cluster
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
