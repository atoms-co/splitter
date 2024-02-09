package core

import (
	"context"
	"atoms.co/lib-go/pkg/clock"
	"go.atoms.co/lib/log"
	"go.atoms.co/lib/net/grpcx"
	"go.atoms.co/splitter/pkg/model"
	"fmt"
	"google.golang.org/grpc"
	"sync"
	"time"
)

type Connection struct {
	GID  GrantID
	Conn *grpc.ClientConn
}

// ServiceResolver resolves a gRPC connection to an instance with service's coordinator. Uses model.ErrNoResolution
// to indicate that service is local.
type ServiceResolver = model.Resolver[Connection, model.QualifiedServiceName]

// NewServiceResolver creates a new resolver that finds service's coordinator location and returns a
// connection to that instance. The resolver keeps connections to all known service instances,
// disconnecting instances that have no coordinators after a minute. Thread-safe.
func NewServiceResolver(ctx context.Context, cl clock.Clock, self model.Instance, clusters <-chan Cluster, opts ...grpc.DialOption) ServiceResolver {
	r := &resolver{
		cl:       cl,
		self:     self,
		peers:    map[model.InstanceID]*grpc.ClientConn{},
		inactive: map[model.InstanceID]inactive{},
	}
	go r.process(ctx, clusters, opts)

	return r
}

type inactive struct {
	worker     model.Instance
	cc         *grpc.ClientConn
	expiration time.Time
}

type resolver struct {
	cl   clock.Clock
	self model.Instance

	inactive map[model.InstanceID]inactive

	peers   map[model.InstanceID]*grpc.ClientConn
	cluster Cluster
	mu      sync.RWMutex
}

func (r *resolver) Resolve(ctx context.Context, service model.QualifiedServiceName) (Connection, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	c, ok := r.cluster.Lookup(service)
	if !ok {
		return Connection{}, model.ErrNotFound
	}

	wid := c.Worker.ID()
	gid := c.Grant.ID()

	if wid == r.self.ID() {
		return Connection{GID: gid}, model.ErrNoResolution
	}
	cc, ok := r.peers[wid]
	if !ok {
		return Connection{GID: gid}, fmt.Errorf("internal: no connection to %v", c.Worker)
	}
	return Connection{GID: gid, Conn: cc}, nil
}

func (r *resolver) process(ctx context.Context, clusters <-chan Cluster, opts []grpc.DialOption) {
	ticker := r.cl.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case cluster, ok := <-clusters:
			if !ok {
				return
			}

			// (1) New cluster. Establish new connections, if any.

			// log.Debugf(ctx, "Received cluster: %v", cluster.ID())

			r.mu.RLock()
			old := r.peers
			r.mu.RUnlock()

			active := map[model.InstanceID]*grpc.ClientConn{}

			workers := cluster.Workers()
			for id, w := range workers {
				if cc, ok := old[id]; ok {
					active[id] = cc // keep
					continue
				}
				if info, ok := r.inactive[id]; ok {
					active[id] = info.cc // re-activate
					delete(r.inactive, id)
					continue
				}

				cc, err := grpcx.DialNonBlocking(ctx, w.Endpoint(), opts...)
				if err != nil {
					// Highly unexpected. A new cluster update will force a retry. We choose not to panic.

					log.Errorf(ctx, "CRITICAL: Failed to create connection to %v: %v", w, err)
					continue
				}
				active[id] = cc
			}

			// (2) Find inactive connections.

			for id, cc := range old {
				if _, ok := active[id]; !ok {
					r.inactive[id] = inactive{worker: workers[id], cc: cc, expiration: r.cl.Now().Add(time.Minute)}
				}
			}

			// (3) Swap cluster map and peer connections.

			r.mu.Lock()
			r.peers = active
			r.cluster = cluster
			r.mu.Unlock()

		case <-ticker.C:
			// Remove old stale connections. We delay a min to handle partial cluster updates
			// from a new leader until all workers have re-registered.

			if len(r.inactive) == 0 {
				break
			}

			now := r.cl.Now()

			r.mu.Lock()
			for id, info := range r.inactive {
				if now.After(info.expiration) {
					continue // skip
				}

				log.Debugf(ctx, "Closing connection to inactive worker: %v", info.worker)
				_ = info.cc.Close()
				delete(r.inactive, id)
			}
			r.mu.Unlock()
		}
	}
}
