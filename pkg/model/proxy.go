package model

import (
	"context"
	"go.atoms.co/lib/backoffx"
	"go.atoms.co/lib/contextx"
	"go.atoms.co/lib/iox"
	"errors"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"time"
)

var (
	// ErrNotOwned is used by Splitter client to indicate that the key is not owned by an instance. This error is used
	// (1) when the key is not owned by the local instance (when final destination of forwarding checks for ownership),
	// and (2) when the ownership of the key cannot be determined (e.g. if cluster information is not available or
	// key domain is not present in it).
	// The recommended way to use this error is to wrap it in a gRPC error (using OwnershipErrorToGRPC) and convert
	// it back to ErrNotOwned (using OwnershipErrorFromGRPC). This allows to use IsOwnershipError and RetryOwnership1
	// to handle retries on ownership changes.
	ErrNotOwned = errors.New("not owned")

	// ErrDraining is not returned by Splitter client, but can be used by the client to indicate that the instance is
	// draining and the forwarding instance should re-try the request.
	// The recommended way to use this error is to wrap it in a gRPC error (using OwnershipErrorToGRPC) and convert
	// it back to ErrDraining (using OwnershipErrorFromGRPC). This allows to use IsOwnershipError and RetryOwnership1
	// to handle retries when instances are drained.
	ErrDraining = errors.New("draining")

	// ErrNoResolution is returned when no resolution is possible, e.g. due to local ownership. This error
	// should only be handled by clients that use the resolver.
	ErrNoResolution = errors.New("no resolution")
)

// IsOwnershipError returns true if the error is likely due to imminent or ongoing shift in ownership,
// incl. grpc Unavailable.
func IsOwnershipError(err error) bool {
	if st, ok := status.FromError(err); ok && st.Code() == codes.Unavailable {
		return true
	}
	return errors.Is(err, ErrDraining) || errors.Is(err, ErrNotOwned)
}

// RetryOwnership1 is a retry wrapper for ownership errors only.
func RetryOwnership1[T any](ctx context.Context, duration time.Duration, fn func(ctx context.Context) (T, error)) (T, error) {
	wctx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()

	b := backoffx.NewLimited(duration, backoffx.WithInitialInterval(time.Second), backoffx.WithMaxInterval(5*time.Second))

	return backoffx.Retry1(b, func() (T, error) {
		t, err := fn(wctx)
		if contextx.IsCancelled(wctx) {
			return t, backoffx.ErrPermanent(err) // don't retry: cancelled
		}
		oerr, _ := OwnershipErrorFromGRPC(err)
		if !IsOwnershipError(oerr) {
			return t, backoffx.ErrPermanent(err) // don't retry: not ownership error
		}
		return t, err
	})
}

// OwnershipErrorToGRPC wraps a logic ownership error into a grpc error.
func OwnershipErrorToGRPC(err error) (error, bool) {
	if err == nil {
		return nil, true
	}
	switch {
	case errors.Is(err, ErrNotOwned):
		return status.Error(codes.OutOfRange, err.Error()), true // code used internally for serialization only
	case errors.Is(err, ErrDraining):
		return status.Error(codes.Aborted, err.Error()), true // code used internally for serialization only
	}
	return err, false
}

// OwnershipErrorFromGRPC recovers a logic ownership error from a grpc error.
func OwnershipErrorFromGRPC(err error) (error, bool) {
	if err == nil {
		return nil, true
	}
	st, ok := status.FromError(err)
	if !ok {
		return err, false
	}

	switch st.Code() {
	case codes.OutOfRange:
		return ErrNotOwned, true // code used internally for serialization only
	case codes.Aborted:
		return ErrDraining, true // code used internally for serialization only
	}
	return err, false
}

// SimpleResolver resolves ownership of a key of type K to a proxy object of type T. Each proxy is typically instantiated
// with a grpc service client, such as Proxy[v1.FooServiceClient] for remote invocation only.
type SimpleResolver[T, K any] interface {
	// Resolve resolves ownership of a key of type K to a proxy object of type T. Returns ErrNoResolution if
	// resolution fails (possibly due to local ownership) or ErrNotFound when information about owner cannot be found
	// (e.g. when key is not owned).
	Resolve(ctx context.Context, key K) (T, error)
}

// Resolver is a resolver that can map a key to a domain key.
type Resolver[T, K any] interface {
	SimpleResolver[T, K]

	DomainKey(key K) QualifiedDomainKey
}

// GRPCMethod is a function signature for invoking a method on a gPRC client, e.g. v1.FooServiceClient.Handle
type GRPCMethod[T, A, B any] func(T, context.Context, A, ...grpc.CallOption) (B, error)

// Invoke makes a grpc invocation to the owner of the given key, if remote, and calls the
// given fallback function if resolution fails (likely due to local owner). The fallback function
// may use a different signature and by unrelated to grpc. It is called only on no resolution.
// Any retry -- notably on ErrNotOwned -- should re-resolve the owner.
//
// For example, using Resolver[v1.FooServiceClient, K], the extended call looks like the following:
//
//	 parsed := parse(req)
//	 ... determine key ...
//		resp, err := splitter.Invoke(ctx, proxy, key, v1.FooServiceClient.Info, req, func() (*v1.InfoResponse, error) {
//	     return local.Info(parsed, ...)
//	 })
//
// Deprecated: Use Handle instead.
func Invoke[T, K, A, B any](ctx context.Context, p Resolver[T, K], key K, fn GRPCMethod[T, A, B], a A, local func() (B, error)) (B, error) {
	t, err := p.Resolve(ctx, key)
	if err != nil {
		if errors.Is(err, ErrNoResolution) {
			rt, err := local()
			recordHandledRequest(ctx, p.DomainKey(key).Domain, "local", err)
			return rt, err
		}
		var b B
		return b, err
	}
	rt, err := fn(t, ctx, a)
	recordHandledRequest(ctx, p.DomainKey(key).Domain, "remote", err)
	return rt, err
}

// RemoteFn is a method for creating a new gRPC client from a grpc.ClientConnInterface.
// Generally use proto generated "NewXXXClient"
type RemoteFn[T any] func(grpc.ClientConnInterface) T

// NOTE(herohde) 9/4/2023: the unification in Splitter1 of local/remote T created some clunky grpc wrappers
// with re-serialization. To avoid the need for those, we move the local check out of the proxy.
// The Invoke is an attempt at what a unified call may look like. Use func(ctx, a) (B, error) to
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
				if errors.Is(err, ErrNoResolution) {
					recordForwardedRequest(ctx, key.Domain, "local", "ok")
				} else {
					recordForwardedRequest(ctx, key.Domain, "remote", err.Error())
				}
				return zero, err
			}
			recordForwardedRequest(ctx, key.Domain, "remote", "ok")
			return r.fn(con), nil
		}
		recordForwardedRequest(ctx, key.Domain, "unknown", "owner_not_found")
		return zero, fmt.Errorf("no owner: %w", ErrNotOwned)
	}

	recordForwardedRequest(ctx, key.Domain, "unknown", "not_initialized")
	return zero, fmt.Errorf("not initialized: %w", ErrNotOwned)
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

// GrantResolver provides access to the local grant-owning V-typed values of a domain.
// The K-typed keys are mapped to a DomainKey to determine grant ownership.
type GrantResolver[K, V any] interface {
	// Lookup returns the value owning the key, if local.
	Lookup(key K, grants ...GrantState) (V, bool)

	DomainKey(key K) QualifiedDomainKey
}

// Proxy provides access to the grant-owning V-typed values of a domain, whether local or remote. It also
// includes the Cluster. The K-typed keys are mapped to a DomainKey to determine grant ownership. Local
// ownership is directly available for local-only uses, such as peer server implementation.
type Proxy[T, K, V any] interface {
	GrantResolver[K, V]
	Resolver[T, K]

	// Cluster returns the Cluster if present. It matches local Range lifecycle, modulo timing and failures.
	Cluster() (Cluster, bool)
}

// ProxyStub is a late-bound Proxy. Useful as indirection for domains that depend on each other, i.e., each
// range factory need a proxy for the other domain. The stub breaks the initialization cycle.
type ProxyStub[T, K, V any] struct {
	proxy       Proxy[T, K, V]
	initialized iox.AsyncCloser
}

func NewProxyStub[T, K, V any]() *ProxyStub[T, K, V] {
	return &ProxyStub[T, K, V]{
		initialized: iox.NewAsyncCloser(),
	}
}

func (p *ProxyStub[T, K, V]) Init(real Proxy[T, K, V]) Proxy[T, K, V] {
	p.proxy = real
	p.initialized.Close()
	return real
}

func (p *ProxyStub[T, K, V]) Lookup(key K, grants ...GrantState) (V, bool) {
	<-p.initialized.Closed()
	return p.proxy.Lookup(key, grants...)
}

func (p *ProxyStub[T, K, V]) Cluster() (Cluster, bool) {
	<-p.initialized.Closed()
	return p.proxy.Cluster()
}

func (p *ProxyStub[T, K, V]) Resolve(ctx context.Context, key K) (T, error) {
	<-p.initialized.Closed()
	return p.proxy.Resolve(ctx, key)
}

func (p *ProxyStub[T, K, V]) DomainKey(key K) QualifiedDomainKey {
	<-p.initialized.Closed()
	return p.proxy.DomainKey(key)
}

// Handle makes a grpc invocation to the owner of the given key, if remote, and calls the given
// fallback function with the grant-owning value if locally owned. May returns ErrNotOwned if
// the grant is in transition. Relies on Invoke for resolution.
//
// For example, using Proxy[v1.FooServiceClient, K, V], a call looks like the following:
//
//	parsed := parse(req)
//	... determine key ...
//	resp, err := splitter.Handle(ctx, proxy, key, v1.FooServiceClient.Info, req, func(v V) (*v1.InfoResponse, error) {
//	    return v.Info(parsed, ...)
//	})
func Handle[K, T, A, B any, V Range](ctx context.Context, p Proxy[T, K, V], key K, fn GRPCMethod[T, A, B], a A, local func(V) (B, error)) (B, error) {
	// Check if a grant is present locally to guard against a stale cluster map.
	// We have to be careful to not pick a non-owner based on resolution rules,
	// so we look up using ACTIVE only. Otherwise, an UNLOADED local range will
	// be picked over a remote LOADED, which is suboptimal.

	if r, ok := p.Lookup(key, ActiveGrantState); ok {
		domain := p.DomainKey(key).Domain
		recordForwardedRequest(ctx, domain, "local", "ok")
		rt, err := local(r)
		recordHandledRequest(ctx, domain, "local", err)
		return rt, err
	}

	return Invoke(ctx, p, key, fn, a, func() (B, error) {
		if r, ok := p.Lookup(key); ok {
			return local(r)
		}
		var b B
		return b, ErrNotOwned
	})
}

// HandleLocal finds a local handler for a key and invokes a handler with the owner.
// Returns ErrNotOwned if the key is not owned locally.
func HandleLocal[K, V, REQ, RESP any](ctx context.Context, r GrantResolver[K, V], key K, req REQ, handler func(V, context.Context, REQ) (RESP, error)) (RESP, error) {
	domain := r.DomainKey(key).Domain
	if g, ok := r.Lookup(key); ok {
		rt, err := handler(g, ctx, req)
		recordHandledRequest(ctx, domain, "local", err)
		return rt, err
	}
	var resp RESP
	recordHandledRequest(ctx, domain, "local", ErrNotOwned)
	return resp, ErrNotOwned
}
