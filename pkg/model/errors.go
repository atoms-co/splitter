package model

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.atoms.co/lib/backoffx"
)

var (
	ErrNotFound        = errors.New("not found")
	ErrAlreadyExists   = errors.New("already exists")
	ErrVersionMismatch = errors.New("version mismatch")
	ErrNotAllowed      = errors.New("not allowed")
	ErrInvalid         = errors.New("invalid")
	ErrOverloaded      = errors.New("overloaded")
)

// IsPermanentError returns true iff the error is one of the defined permanent errors.
func IsPermanentError(err error) bool {
	switch {
	case errors.Is(err, ErrNotFound), errors.Is(err, ErrAlreadyExists), errors.Is(err, ErrVersionMismatch), errors.Is(err, ErrNotAllowed), errors.Is(err, ErrInvalid):
		return true
	default:
		return false
	}
}

// BackoffError wraps permanent errors as such for backoffx to avoid further retries.
func BackoffError(err error) error {
	if IsPermanentError(err) {
		return backoffx.ErrPermanent(err)
	}
	return err
}

// ToGRPCError wraps a logic error into a grpc error code.
func ToGRPCError(err error) error {
	if err, ok := OwnershipErrorToGRPC(err); ok {
		return err
	}
	switch {
	case errors.Is(err, ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, ErrAlreadyExists):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, ErrVersionMismatch):
		fallthrough
	case errors.Is(err, ErrNotAllowed):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, ErrInvalid):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, ErrOverloaded):
		return status.Error(codes.ResourceExhausted, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return err
	case errors.Is(err, context.Canceled):
		return err
	default:
		if _, ok := status.FromError(err); ok {
			return err // ok: already a grpc error
		}
		return status.Error(codes.Internal, err.Error())
	}
}

// FromGRPCError recovers a logic error from a grpc error code.
func FromGRPCError(err error) error {
	if err, ok := OwnershipErrorFromGRPC(err); ok {
		return err
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
	default:
		return err
	}
}
