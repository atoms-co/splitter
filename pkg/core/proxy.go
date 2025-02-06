package core

import (
	"context"
	"go.atoms.co/splitter/pkg/model"
	"errors"
)

// Invoke makes a grpc invocation to the owner of the given key, if remote, and calls the
// given fallback function if resolution fails (likely due to local owner). The fallback function
// may use a different signature and by unrelated to grpc. It is called only on no resolution.
// Any retry -- notably on ErrNotOwned -- should re-resolve the owner.
func Invoke[T, K, A, B any](ctx context.Context, p model.SimpleResolver[T, K], key K, fn model.GRPCMethod[T, A, B], a A, local func() (B, error)) (B, error) {
	t, err := p.Resolve(ctx, key)
	if err != nil {
		if errors.Is(err, model.ErrNoResolution) {
			return local()
		}
		var b B
		return b, err
	}
	return fn(t, ctx, a)
}

// InvokeZero is an Invoke convenience wrapper using ZeroDomainKey. Suitable for Unit domains.
func InvokeZero[T, A, B any](ctx context.Context, p model.SimpleResolver[T, model.DomainKey], fn model.GRPCMethod[T, A, B], a A, local func() (B, error)) (B, error) {
	return Invoke(ctx, p, model.ZeroDomainKey, fn, a, local)
}
