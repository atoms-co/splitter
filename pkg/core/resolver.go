package core

import (
	"context"
	"go.atoms.co/lib/log"
	"go.atoms.co/splitter/pkg/model"
	"google.golang.org/grpc"
	"sync"
)

type Connection struct {
	GID  GrantID
	Conn *grpc.ClientConn
}

// TenantResolver resolves a gRPC connection to an instance with tenant's coordinator. Uses model.ErrNoResolution
// to indicate that tenant is local.
type TenantResolver = model.Resolver[Connection, model.TenantName]

// NewTenantResolver creates a new resolver that finds tenant's coordinator location and returns a connection to that instance.
// The resolver keeps connections to all known tenant instances, disconnecting instances that have no coordinators.
// Thread-safe.
func NewTenantResolver(ctx context.Context, self model.Instance, clusters <-chan Cluster, opts ...grpc.DialOption) TenantResolver {
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

func (r *resolver) Resolve(ctx context.Context, tenant model.TenantName) (Connection, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	c, ok := r.cluster.Tenant(tenant)
	if !ok {
		return Connection{}, model.ErrInvalid
	}
	if c.instance.Endpoint() == r.self.Endpoint() {
		return Connection{}, model.ErrNoResolution
	}
	return Connection{
		GID:  c.gid,
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

		for tenant, c := range cluster.Tenants() {
			if c.instance.ID() == r.self.ID() {
				continue // ignore self
			}
			endpoint := c.instance.Endpoint()
			if cc, ok := old[endpoint]; ok {
				upd[endpoint] = cc
				continue
			}

			log.Debugf(ctx, "Opening connection to %v located at coordinator %v", tenant, c)

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
