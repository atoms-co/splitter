// Package storage package contains abstractions and utilities for working with storage
package storage

import "go.atoms.co/splitter/pkg/model"

// UpdateGuard holds an expected version for metadata. An update will only go through if the current version
// matches the guard.
type UpdateGuard struct {
	Guard model.Version
}
