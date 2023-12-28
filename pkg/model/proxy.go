package model

import (
	"context"
	"go.atoms.co/lib/log"
	"go.atoms.co/lib/metrics"
	"go.atoms.co/lib/net/grpcx"
	"errors"
	"fmt"
	"google.golang.org/grpc"
	"sync"
)

var (
	ErrNoResolution = errors.New("no resolution")
)

// Resolver resolves ownership of a key of type K to a proxy object of type T. Each proxy is typically instantiated
// with a grpc service client, such as Proxy[v1.FooServiceClient] for remote invocation only. Returns ErrNoResolution
// if resolution fails (possibly due to local ownership) and ErrInvalid if the key is not valid.
type Resolver[T, K any] interface {
	Resolve(ctx context.Context, key K) (T, error)
}

// Invoke makes a grpc invocation to the owner of the given key. This convenience function mimics
// a normal grpc invocation and allows the generalized version to be a single line of code.
//
// For example, using Proxy[v1.FooServiceClient], the following becomes equivalent:
//
//	resp, err := client.Info(ctx, req)
//	resp, err := splitter.Invoke(ctx, proxy, key, v1.FooServiceClient.Info, req)
func Invoke[T, K, A, B any](ctx context.Context, p Resolver[T, K], key K, fn func(T, context.Context, A, ...grpc.CallOption) (B, error), a A) (B, error) {
	t, err := p.Resolve(ctx, key)
	if err != nil {
		var b B
		return b, err
	}
	return fn(t, ctx, a)
}

// InvokeZero is an Invoke convenience wrapper using ZeroDomainKey. Suitable for Unit domains.
func InvokeZero[T, A, B any](ctx context.Context, p Resolver[T, DomainKey], fn func(T, context.Context, A, ...grpc.CallOption) (B, error), a A) (B, error) {
	return Invoke(ctx, p, ZeroDomainKey, fn, a)
}

// InvokeEx makes a grpc invocation to the owner of the given key, if remote, and calls the
// given fallback function if resolution fails (likely due to local owner). The fallback function
// may use a different signature and by unrelated to grpc. It is called only on no resolution.
// Any retry -- notably on ErrNotOwned -- should re-resolve the owner.
//
// For example, using Proxy[v1.FooServiceClient], the extended call looks like the following:
//
//  parsed := parse(req)
//  ... determine key ...
//	resp, err := splitter.InvokeEx(ctx, proxy, key, v1.FooServiceClient.Info, req, func() (*v1.InfoResponse, error) {
//      return local.Info(parsed, ...)
//  })
func InvokeEx[T, K, A, B any](ctx context.Context, p Resolver[T, K], key K, fn func(T, context.Context, A, ...grpc.CallOption) (B, error), a A, local func() (B, error)) (B, error) {
	t, err := p.Resolve(ctx, key)
	if err != nil {
		if errors.Is(err, ErrNoResolution) {
			return local()
		}
		var b B
		return b, err
	}
	return fn(t, ctx, a)
}

// InvokeExZero is an InvokeEx convenience wrapper using ZeroDomainKey. Suitable for Unit domains.
func InvokeExZero[T, A, B any](ctx context.Context, p Resolver[T, DomainKey], fn func(T, context.Context, A, ...grpc.CallOption) (B, error), a A, local func() (B, error)) (B, error) {
	return InvokeEx(ctx, p, ZeroDomainKey, fn, a, local)
}

type RemoteFn[T any] func(grpc.ClientConnInterface) T

// NOTE(herohde) 9/4/2023: the unification in Splitter1 of local/remote T created some clunky grpc wrappers
// with re-serialization. To avoid the need for those, we move the local check out of the proxy.
// The InvokeEx is an attempt at what a unified call may look like. Use func(ctx, a) (B, error) to
// not force a closure as a variant?

// Connection represents an addressable instance and an optional grpc connection to its endpoint.
type Connection struct {
	Instance Instance
	Conn     grpc.ClientConnInterface // nil if local
}

// ConnectionPool maintains connections to various instances for redirection. Multiple
// proxies with different service contracts can use the same resolver. Thead-safe.
type ConnectionPool interface {
	Connect(ctx context.Context, id InstanceID) (Connection, error)
}

var (
	numForwards = metrics.NewCounter("go.atoms.co/splitter/forwarder_requests", "Number of forward requests", resultKey)
)

// resolver is a low-level helper for redirecting requests to the shard owner, using a connection pool. Multiple
// proxies with different service contracts can use the same resolver.
type resolver[T any] struct {
	self InstanceID
	pool ConnectionPool
	fn   RemoteFn[T]

	cluster ClusterProvider
	mu      sync.RWMutex
}

// NewResolver creates a resolve to reach domain owners over grpc. Callers must check if locally owned first.
// If an ownership failure occurs, the local check must be re-done before a retry.
func NewResolver[T any](ctx context.Context, self InstanceID, clusterProvider ClusterProvider, pool ConnectionPool, fn RemoteFn[T]) Resolver[T, QualifiedDomainKey] {
	ret := &resolver[T]{
		self:    self,
		pool:    pool,
		fn:      fn,
		cluster: clusterProvider,
	}
	return ret
}

// Resolve returns a shared grpc connection to the owning instance, if remote. Returns false if local.
func (r *resolver[T]) Resolve(ctx context.Context, key QualifiedDomainKey) (T, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var rt T

	cluster, ok := r.cluster.Cluster()
	if !ok {
		return rt, fmt.Errorf("clustermap not initialized: %w", ErrNotFound)
	}

	if instance, state, ok := cluster.Owner(key); ok && IsActiveGrant(state) {
		if instance.ID() == r.self {
			return rt, ErrNoResolution
		}
		con, err := r.pool.Connect(ctx, instance.ID())
		if err != nil {
			numForwards.Increment(ctx, 1, resultTag("no_connection"))
			return rt, fmt.Errorf("no connection: %w", ErrInvalid) // Should this be Invalid?
		}
		numForwards.Increment(ctx, 1, resultTag("ok"))
		return r.fn(con.Conn), nil
	}
	numForwards.Increment(ctx, 1, resultTag("not_initialized"))
	return rt, fmt.Errorf("not initialized: %w", ErrInvalid) // Should this be Invalid?
}

type connectionPool struct {
	provider ClusterProvider
	opts     []grpc.DialOption

	connections map[InstanceID]Connection
	mu          sync.RWMutex
}

func NewConnectionPool(provider ClusterProvider, opts ...grpc.DialOption) ConnectionPool {
	p := connectionPool{provider: provider, opts: opts, connections: map[InstanceID]Connection{}}
	return &p
}

func (p *connectionPool) Connect(ctx context.Context, id InstanceID) (Connection, error) {
	p.mu.RLock()
	c, ok := p.connections[id]
	p.mu.RUnlock()

	if ok {
		return c, nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	cluster, ok := p.provider.Cluster()
	if !ok {
		return Connection{}, fmt.Errorf("clustermap not initialized: %w", ErrNotFound)
	}

	instance, ok := cluster.Consumer(id)
	if !ok {
		return Connection{}, fmt.Errorf("consumer %v: %w", id, ErrNotFound)
	}

	cc, err := grpcx.DialNonBlocking(ctx, instance.Endpoint(), p.opts...)
	if err != nil {
		log.Errorf(ctx, "Failed to create connection to %v: %v", instance, err)
		return Connection{}, err
	}
	c = Connection{Instance: instance, Conn: cc}
	p.connections[id] = c
	return c, nil
}

// DomainResolver is a convenience wrapper for single-domain resolution using the cluster resolver.
type DomainResolver[T, K any] struct {
	name     QualifiedDomainName
	resolver Resolver[T, QualifiedDomainKey]
	fn       func(K) DomainKey
}

func NewDomainResolver[T, K any](name QualifiedDomainName, resolver Resolver[T, QualifiedDomainKey], fn func(K) DomainKey) *DomainResolver[T, K] {
	return &DomainResolver[T, K]{name: name, resolver: resolver, fn: fn}
}

func (d *DomainResolver[T, K]) Resolve(ctx context.Context, key K) (T, error) {
	return d.resolver.Resolve(ctx, QualifiedDomainKey{Domain: d.name, Key: d.fn(key)})
}

func (d *DomainResolver[T, K]) String() string {
	return d.name.String()
}
