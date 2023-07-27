package model

import (
	"context"
	"go.atoms.co/lib/backoffx"
	"go.atoms.co/lib/contextx"
	"errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"time"
)

const (
	OwnershipTimeout = 30 * time.Second
)

var (
	ErrNotFound        = errors.New("not found")
	ErrAlreadyExists   = errors.New("already exists")
	ErrVersionMismatch = errors.New("version mismatch")
	ErrNotAllowed      = errors.New("not allowed")
	ErrInvalid         = errors.New("invalid")

	ErrOverloaded = errors.New("overloaded")
	ErrNotOwned   = errors.New("not owned")
	ErrDraining   = errors.New("draining")
)

// IsPermanentError returns true iff the error is one of the defined permanent errors.
func IsPermanentError(err error) bool {
	switch err {
	case ErrNotFound, ErrAlreadyExists, ErrVersionMismatch, ErrNotAllowed, ErrInvalid:
		return true
	default:
		return false
	}
}

// IsOwnershipError returns true if the error is likely due to imminent or ongoing shift in ownership,
// incl. grpc Unavailable.
func IsOwnershipError(err error) bool {
	if st, ok := status.FromError(err); ok && st.Code() == codes.Unavailable {
		return true
	}
	return err == ErrDraining || err == ErrNotOwned
}

// BackoffError wraps permanent errors as such for backoffx to avoid further retries.
func BackoffError(err error) error {
	if IsPermanentError(err) {
		return backoffx.ErrPermanent(err)
	}
	return err
}

// WrapError wraps a logic error into a grpc error code.
func WrapError(err error) error {
	if err == nil {
		return nil
	}
	switch err {
	case ErrNotFound:
		return status.Error(codes.NotFound, err.Error())
	case ErrAlreadyExists:
		return status.Error(codes.AlreadyExists, err.Error())
	case ErrVersionMismatch:
		fallthrough
	case ErrNotAllowed:
		return status.Error(codes.FailedPrecondition, err.Error())
	case ErrInvalid:
		return status.Error(codes.InvalidArgument, err.Error())
	case ErrOverloaded:
		return status.Error(codes.ResourceExhausted, err.Error())
	case ErrNotOwned:
		return status.Error(codes.OutOfRange, err.Error()) // code used internally for serialization only
	case ErrDraining:
		return status.Error(codes.Aborted, err.Error()) // code used internally for serialization only
	default:
		if _, ok := status.FromError(err); ok {
			return err // ok: already a grpc error
		}
		return status.Error(codes.Internal, err.Error())
	}
}

// UnwrapError recovers a logic error from a grpc error code.
func UnwrapError(err error) error {
	if err == nil {
		return nil
	}
	st, ok := status.FromError(err)
	if !ok {
		return err
	}

	switch st.Code() {
	case codes.NotFound:
		return ErrNotFound
	case codes.AlreadyExists:
		return ErrAlreadyExists
	case codes.InvalidArgument:
		return ErrInvalid
	case codes.FailedPrecondition:
		return ErrVersionMismatch
	case codes.ResourceExhausted:
		return ErrOverloaded
	case codes.OutOfRange:
		return ErrNotOwned // code used internally for serialization only
	case codes.Aborted:
		return ErrDraining // code used internally for serialization only
	default:
		return err
	}
}

// RetryOwnership1 is a retry wrapper for ownership errors only.
func RetryOwnership1[T any](ctx context.Context, duration time.Duration, fn func(ctx context.Context) (T, error)) (T, error) {
	wctx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()

	b := backoffx.NewLimited(duration, backoffx.WithInitialInterval(time.Second), backoffx.WithMaxInterval(5*time.Second))

	return backoffx.Retry1(b, func() (T, error) {
		t, err := fn(wctx)
		if contextx.IsCancelled(wctx) || !IsOwnershipError(UnwrapError(err)) {
			return t, backoffx.ErrPermanent(err) // don't retry: cancelled or not ownership error
		}
		return t, err
	})
}
