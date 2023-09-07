package model

import (
	"context"
	"google.golang.org/grpc"
)

// Proxy resolves ownership of a key of type K to a proxy object of type T. Each proxy is typically instantiated
// with a grpc service client, such as Proxy[v1.FooServiceClient] for remote invocation. Returns ErrNotOwned
// if resolution fails.
type Proxy[T, K any] interface {
	Resolve(ctx context.Context, key K) (T, error)
}

// Invoke makes a local or remote grpc invocation to the owner of the given key. This convenience function mimics
// a normal grpc invocation and allows the generalized version to be a single line of code.
//
// For example, using Proxy[v1.FooServiceClient], the following becomes equivalent:
//
//	resp, err := client.Info(ctx, req)
//	resp, err := splitter.Invoke(ctx, proxy, key, v1.FooServiceClient.Info, req)
func Invoke[T, K, A, B any](ctx context.Context, p Proxy[T, K], key K, fn func(T, context.Context, A, ...grpc.CallOption) (B, error), a A) (B, error) {
	t, err := p.Resolve(ctx, key)
	if err != nil {
		var b B
		return b, err
	}
	return fn(t, ctx, a)
}

type RemoteFn[T any] func(grpc.ClientConnInterface) T

// NOTE(herohde) 9/4/2023: the unification in Splitter1 of local/remote T created some clunky grpc wrappers
// with re-serialization. To avoid the need for those, we move the local check out of the proxy.

// NewProxy creates a proxy to reach domain owners over grpc. Callers must check if locally owned first.
// If an ownership failure occurs, the local check must be re-done before a retry.
func NewProxy[T any]( /* cluster map */ pool ConnectionPool, fn RemoteFn[T]) Proxy[T, QualifiedDomainKey] {
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
