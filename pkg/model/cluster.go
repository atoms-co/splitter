package model

import (
	"context"
	"sync"
)

type Cluster struct {
}

func (c Cluster) Owner(key QualifiedDomainKey) (Instance, bool) {
	return Instance{}, false
}

func (c Cluster) Consumer(id ConsumerID) (Consumer, bool) {
	return Consumer{}, false
}

func (c Cluster) Consumers() []Consumer {
	return nil
}

func NewCluster() Cluster {
	return Cluster{}
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
