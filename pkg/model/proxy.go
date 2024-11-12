package model

import (
	"context"
	"errors"
	"fmt"
	"google.golang.org/grpc"
)

var (
	ErrNoResolution = errors.New("no resolution")
)

// SimpleResolver resolves ownership of a key of type K to a proxy object of type T. Each proxy is typically instantiated
// with a grpc service client, such as Proxy[v1.FooServiceClient] for remote invocation only. Returns ErrNoResolution
// if resolution fails (possibly due to local ownership), ErrInvalid if the key is not valid, or ErrNotFound when
// information about owner cannot be found (e.g. when key is not owned).
type SimpleResolver[T, K any] interface {
	Resolve(ctx context.Context, key K) (T, error)
}

// Resolver is a resolver that can map a key to a domain key.
type Resolver[T, K any] interface {
	SimpleResolver[T, K]

	DomainKey(key K) QualifiedDomainKey
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
// For example, using Resolver[v1.FooServiceClient, K], the extended call looks like the following:
//
//	 parsed := parse(req)
//	 ... determine key ...
//		resp, err := splitter.InvokeEx(ctx, proxy, key, v1.FooServiceClient.Info, req, func() (*v1.InfoResponse, error) {
//	     return local.Info(parsed, ...)
//	 })
func InvokeEx[T, K, A, B any](ctx context.Context, p Resolver[T, K], key K, fn func(T, context.Context, A, ...grpc.CallOption) (B, error), a A, local func() (B, error)) (B, error) {
	t, err := p.Resolve(ctx, key)
	if err != nil {
		if errors.Is(err, ErrNoResolution) {
			rt, err := local()
			if err != nil {
				recordHandledRequest(ctx, p.DomainKey(key).Domain, err.Error())
			} else {
				recordHandledRequest(ctx, p.DomainKey(key).Domain, "ok")
			}
			return rt, err
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

// RemoteFn is a method for creating a new gRPC client from a grpc.ClientConnInterface.
// Generally use proto generated "NewXXXClient"
type RemoteFn[T any] func(grpc.ClientConnInterface) T

// NOTE(herohde) 9/4/2023: the unification in Splitter1 of local/remote T created some clunky grpc wrappers
// with re-serialization. To avoid the need for those, we move the local check out of the proxy.
// The InvokeEx is an attempt at what a unified call may look like. Use func(ctx, a) (B, error) to
// not force a closure as a variant?

// resolver is a low-level helper for redirecting requests to the shard owner, using a connection pool. Multiple
// proxies with different service contracts can use the same resolver.
type resolver[T any] struct {
	pool   ConnectionPool
	fn     RemoteFn[T]
	states []GrantState // Grant state resolution. Empty if default.
}

// NewResolver creates a resolver to reach domain owners over grpc. If an ownership failure occurs, the
// local check must be re-done before a retry. The resolver uses the default notion of ownership, unless
// a custom list of grant states are provided.
func NewResolver[T any](pool ConnectionPool, fn RemoteFn[T], states ...GrantState) Resolver[T, QualifiedDomainKey] {
	return &resolver[T]{
		pool:   pool,
		fn:     fn,
		states: states,
	}
}

// Resolve returns a shared grpc connection to the owning instance, if remote. Returns ErrNoResolution if local
// or ErrNotFound if owner cannot be determined.
func (r *resolver[T]) Resolve(ctx context.Context, key QualifiedDomainKey) (T, error) {
	var zero T

	if c, ok := r.pool.Cluster(); ok {
		if instance, _, ok := c.Lookup(key, r.states...); ok {
			con, err := r.pool.Resolve(ctx, instance)
			if err != nil {
				recordForwardedRequest(ctx, key.Domain, err.Error())
				return zero, err
			}
			recordForwardedRequest(ctx, key.Domain, "ok")
			return r.fn(con), nil
		}
		recordForwardedRequest(ctx, key.Domain, "owner_not_found")
		return zero, fmt.Errorf("no owner: %w", ErrNotFound)
	}

	recordForwardedRequest(ctx, key.Domain, "not_initialized")
	return zero, fmt.Errorf("not initialized: %w", ErrNotFound)
}

func (r *resolver[T]) DomainKey(key QualifiedDomainKey) QualifiedDomainKey {
	return key
}

type domainResolver[T, K any] struct {
	resolver Resolver[T, QualifiedDomainKey]
	fn       func(K) QualifiedDomainKey
}

// NewDomainResolver is a convenience wrapper for custom key resolution, such as single domains with a uuid key.
func NewDomainResolver[T, K any](pool ConnectionPool, fn RemoteFn[T], kfn func(K) QualifiedDomainKey, states ...GrantState) Resolver[T, K] {
	return &domainResolver[T, K]{resolver: NewResolver(pool, fn, states...), fn: kfn}
}

func (d *domainResolver[T, K]) Resolve(ctx context.Context, key K) (T, error) {
	return d.resolver.Resolve(ctx, d.fn(key))
}

func (d *domainResolver[T, K]) DomainKey(key K) QualifiedDomainKey {
	return d.fn(key)
}
