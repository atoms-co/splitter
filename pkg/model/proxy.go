package model

import (
	"context"
	"errors"
	"google.golang.org/grpc"
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
		if err == ErrNoResolution {
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

// NewResolver creates a proxy to reach domain owners over grpc. Callers must check if locally owned first.
// If an ownership failure occurs, the local check must be re-done before a retry.
func NewResolver[T any]( /* cluster map */ pool ConnectionPool, fn RemoteFn[T]) Resolver[T, QualifiedDomainKey] {
	return nil
}

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
