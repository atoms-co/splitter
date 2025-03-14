package core

import (
	"context"
	"fmt"
	"sync"

	"google.golang.org/grpc"

	"atoms.co/lib-go/pkg/clock"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pb/private"
)

// ServiceResolver resolves a gRPC connection to an instance with service's coordinator. Uses model.ErrNoResolution
// to indicate that service is local.
type ServiceResolver = model.SimpleResolver[internal_v1.CoordinatorServiceClient, model.QualifiedServiceName]

type resolver struct {
	pool *model.PeeredConnectionCache[grpc.ClientConnInterface]

	cluster *Cluster
	mu      sync.RWMutex
}

// NewServiceResolver creates a new resolver that finds service's coordinator location and returns a
// connection to that instance. The resolver maintains a connection pool. Thread-safe.
func NewServiceResolver(ctx context.Context, cl clock.Clock, self model.InstanceID, clusters <-chan *Cluster, opts ...grpc.DialOption) ServiceResolver {
	r := &resolver{
		pool: model.NewPeeredConnectionCache[grpc.ClientConnInterface](ctx, cl, self, model.DialNonBlocking(opts...)),
	}
	go r.process(ctx, clusters)

	return r
}

func (r *resolver) Resolve(ctx context.Context, service model.QualifiedServiceName) (internal_v1.CoordinatorServiceClient, error) {
	cluster, ok := r.Cluster()
	if !ok {
		return nil, fmt.Errorf("not initialized: %w", model.ErrNotFound)
	}
	c, ok := cluster.Lookup(service)
	if !ok {
		return nil, model.ErrNotFound
	}

	cc, err := r.pool.Resolve(ctx, c.Worker)
	if err != nil {
		return nil, err
	}
	return internal_v1.NewCoordinatorServiceClient(cc), nil
}

func (r *resolver) Cluster() (*Cluster, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.cluster, r.cluster != nil
}

func (r *resolver) process(ctx context.Context, clusters <-chan *Cluster) {
	for {
		select {
		case cluster, ok := <-clusters:
			if !ok {
				return
			}

			r.pool.Update(ctx, cluster.Workers())

			r.mu.Lock()
			r.cluster = cluster
			r.mu.Unlock()

		case <-ctx.Done():
			return
		}
	}
}
