package core

import (
	"context"
	"go.atoms.co/lib/log"
	"go.atoms.co/splitter/pkg/model"
	"fmt"
	"google.golang.org/grpc"
	"sync"
)

type Connection struct {
	GID  GrantID
	Conn *grpc.ClientConn
}

// ServiceResolver resolves a gRPC connection to an instance with service's coordinator. Uses model.ErrNoResolution
// to indicate that service is local.
type ServiceResolver = model.Resolver[Connection, model.QualifiedServiceName]

// NewServiceResolver creates a new resolver that finds service's coordinator location and returns a connection to that instance.
// The resolver keeps connections to all known service instances, disconnecting instances that have no coordinators.
// Thread-safe.
func NewServiceResolver(ctx context.Context, self model.Instance, clusters <-chan Cluster, opts ...grpc.DialOption) ServiceResolver {
	r := &resolver{
		self:  self,
		peers: map[string]*grpc.ClientConn{},
	}
	go r.process(ctx, clusters, opts)
	return r
}

type resolver struct {
	self model.Instance

	peers   map[string]*grpc.ClientConn
	cluster Cluster
	mu      sync.RWMutex
}

func (r *resolver) Resolve(ctx context.Context, service model.QualifiedServiceName) (Connection, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	c, ok := r.cluster.Service(service)
	if !ok {
		return Connection{}, fmt.Errorf("no coordinator found for service %v: %w", service, model.ErrInvalid)
	}
	if c.instance.Endpoint() == r.self.Endpoint() {
		return Connection{GID: c.grant.ID()}, model.ErrNoResolution
	}
	return Connection{
		GID:  c.grant.ID(),
		Conn: r.peers[c.instance.Endpoint()],
	}, nil
}

func (r *resolver) process(ctx context.Context, clusters <-chan Cluster, opts []grpc.DialOption) {
	for cluster := range clusters {
		// (1) New cluster. Establish new connections, if any.

		log.Debugf(ctx, "Received cluster: %v", cluster)

		r.mu.RLock()
		old := r.peers
		r.mu.RUnlock()

		upd := map[string]*grpc.ClientConn{}

		for service, c := range cluster.Services() {
			if c.instance.ID() == r.self.ID() {
				continue // ignore self
			}
			endpoint := c.instance.Endpoint()
			if cc, ok := old[endpoint]; ok {
				upd[endpoint] = cc
				continue
			}

			log.Debugf(ctx, "Opening connection to %v located at coordinator %v", service, c)

			cc, err := grpc.DialContext(ctx, endpoint, opts...)
			if err != nil {
				// Highly unexpected. A new cluster update will force a retry. We choose not to panic.

				log.Errorf(ctx, "CRITICAL: Failed to create connection to %v: %v", c, err)
				continue
			}
			upd[endpoint] = cc
		}

		// (2) Swap state. Remove old connections.

		r.mu.Lock()
		r.peers = upd
		r.cluster = cluster
		r.mu.Unlock()

		for endpoint, cc := range old {
			if _, ok := upd[endpoint]; !ok {
				log.Debugf(ctx, "Closing connection to %v", endpoint)
				_ = cc.Close()
			}
		}
	}
}
