// Package storage package contains abstractions and utilities for working with storage
package storage

import (
	"context"
)

// Storage provides low-level access to Splitter storage. The information is expected to be small with
// all values cached in memory by the leader. It caters to log-based storage. Thread-safe.
type Storage interface {
	// Read returns a strongly-consistent snapshot of all persisted data.
	Read(ctx context.Context) (Snapshot, error)
	// Update applies the Update. Returns ErrNotAllowed if not suitable for the current state.
	Update(ctx context.Context, update Update) error
	// Delete applies the Delete. Returns ErrNotFound if the tenant does not exist.
	Delete(ctx context.Context, del Delete) error
}
