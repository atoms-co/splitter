package model

import (
	"context"
	"fmt"
	"io"
	"slices"
	"sync"
	"time"

	"google.golang.org/grpc"

	"atoms.co/lib-go/pkg/clock"
	"go.atoms.co/splitter/lib/service/location"
	"go.atoms.co/lib/log"
	"go.atoms.co/lib/net/grpcx"
)

// DialFn returns a separately-closable T connection. Must be non-blocking.
type DialFn[T any] func(endpoint string) (io.Closer, T, error)

type connection[T any] struct {
	peer Instance
	quit io.Closer
	cc   T
	ttl  time.Time // zero if indefinite
}

// PeeredConnectionCache is a pool of reusable connections of type T  to a dynamic set of peers. Connections to
// peers in the mesh are made easily and reused, assuming long-running streaming usage. When a peer is gone
// for 2 min, its connection is closed. This delay support temporary partial peer lists. Supports ad-hoc
// connection created by considering them peers. Thread-safe.
type PeeredConnectionCache[T any] struct {
	cl   clock.Clock
	self InstanceID
	fn   DialFn[T]

	peers map[InstanceID]connection[T]
	mu    sync.RWMutex
}

func NewPeeredConnectionCache[T any](ctx context.Context, cl clock.Clock, self InstanceID, fn DialFn[T]) *PeeredConnectionCache[T] {
	ret := &PeeredConnectionCache[T]{
		cl:    cl,
		self:  self,
		fn:    fn,
		peers: map[InstanceID]connection[T]{},
	}
	go ret.process(ctx)

	return ret
}

func (p *PeeredConnectionCache[T]) Resolve(ctx context.Context, inst Instance) (T, error) {
	var zero T
	if inst.ID() == p.self {
		return zero, ErrNoResolution
	}

	// (1) Fast check

	p.mu.RLock()
	con, ok := p.peers[inst.ID()]
	p.mu.RUnlock()

	if ok {
		return con.cc, nil
	}

	// (2) Slow check. Dial under lock to prevent concurrent dial spike to the same ad-hoc peer.

	p.mu.Lock()
	defer p.mu.Unlock()

	if con, ok := p.peers[inst.ID()]; ok {
		return con.cc, nil
	}

	quit, cc, err := p.dial(ctx, "adhoc", inst)
	if err != nil {
		return zero, err
	}
	p.peers[inst.ID()] = connection[T]{peer: inst, quit: quit, cc: cc}
	return cc, nil
}

func (p *PeeredConnectionCache[T]) Update(ctx context.Context, peers []Instance) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// (1) New list of peers. Establish new connections, if any.

	upd := map[InstanceID]connection[T]{}

	for _, inst := range peers {
		id := inst.ID()

		if id == p.self {
			continue
		}
		if cur, ok := p.peers[id]; ok {
			upd[id] = connection[T]{peer: inst, quit: cur.quit, cc: cur.cc} // promote to indefinite tll
			continue
		}

		quit, cc, err := p.dial(ctx, "peer", inst)
		if err != nil {
			log.Errorf(ctx, "Internal: %v", err)
			continue
		}
		upd[id] = connection[T]{peer: inst, quit: quit, cc: cc}
	}

	// (2) Set TTL for inactive connections.

	for id, con := range p.peers {
		if _, ok := upd[id]; ok {
			continue
		}

		if con.ttl.IsZero() {
			con.ttl = p.cl.Now().Add(2 * time.Minute)
		}
		upd[id] = con
	}

	// (3) Swap cluster map and peer connections.

	p.peers = upd
}

func (p *PeeredConnectionCache[T]) process(ctx context.Context) {
	ticker := p.cl.NewTicker(20 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			p.expire(ctx)

		case <-ctx.Done():
			return
		}
	}
}

func (p *PeeredConnectionCache[T]) expire(ctx context.Context) {
	p.mu.Lock()
	defer p.mu.Unlock()

	now := p.cl.Now()

	for id, con := range p.peers {
		if con.ttl.IsZero() || now.Before(con.ttl) {
			continue // skip
		}

		log.Infof(ctx, "Closing connection to inactive peer: %v", con.peer)
		_ = con.quit.Close()
		delete(p.peers, id)
	}
}

func (p *PeeredConnectionCache[T]) dial(ctx context.Context, reason string, inst Instance) (io.Closer, T, error) {
	quit, cc, err := p.fn(inst.Endpoint())
	if err != nil {
		// Highly unexpected. Non-blocking dials are not expected to fail.
		var zero T
		return nil, zero, fmt.Errorf("failed to dial %v: %v", inst, err)
	}

	log.Infof(ctx, "Established %v connection to %v", reason, inst)
	return quit, cc, nil
}

// DialNonBlocking64 is a grpc DialFn.
func DialNonBlocking64(opts ...grpc.DialOption) DialFn[grpc.ClientConnInterface] {
	return func(endpoint string) (io.Closer, grpc.ClientConnInterface, error) {
		cc, err := grpcx.DialNonBlocking64(context.Background(), endpoint, slices.Clone(opts)...)
		return cc, cc, err
	}
}

// ConnectionPool maintains the cluster map and its grpc connections. Thread-safe.
type ConnectionPool interface {
	SimpleResolver[grpc.ClientConnInterface, Consumer]
	Cluster() (Cluster, bool)
	Location(key QualifiedDomainKey) (location.Location, bool)
}

type pool struct {
	pool *PeeredConnectionCache[grpc.ClientConnInterface]

	cluster Cluster
	mu      sync.RWMutex
}

func NewConnectionPool(ctx context.Context, cl clock.Clock, self InstanceID, clusters <-chan Cluster, opts ...grpc.DialOption) ConnectionPool {
	p := &pool{
		pool: NewPeeredConnectionCache[grpc.ClientConnInterface](ctx, cl, self, DialNonBlocking64(opts...)),
	}
	go p.process(ctx, clusters)

	return p
}

func (p *pool) Resolve(ctx context.Context, inst Consumer) (grpc.ClientConnInterface, error) {
	return p.pool.Resolve(ctx, inst)
}

func (p *pool) Cluster() (Cluster, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return p.cluster, p.cluster != nil
}

func (p *pool) Location(key QualifiedDomainKey) (location.Location, bool) {
	if c, ok := p.Cluster(); ok {
		if consumer, _, ok := c.Lookup(key); ok {
			return consumer.Location(), true
		}
	}
	return location.Location{}, false
}

func (p *pool) process(ctx context.Context, clusters <-chan Cluster) {
	for {
		select {
		case cluster, ok := <-clusters:
			if !ok {
				return
			}

			p.pool.Update(ctx, cluster.Consumers())

			p.mu.Lock()
			p.cluster = cluster
			p.mu.Unlock()

		case <-ctx.Done():
			return
		}
	}
}
