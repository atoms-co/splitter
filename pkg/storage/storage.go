// Package storage package contains abstractions and utilities for working with storage
package storage

import (
	"context"
	"go.atoms.co/splitter/pkg/core"
	"go.atoms.co/splitter/pkg/model"
)

// Storage provides access to the Splitter storage. The information is expected to be small with all values cached
// in memory by the leader. Thread-safe.
type Storage interface {
	Tenants() Tenants
	Domains() Domains
	Placements() Placements
}

// Tenants is a versioned tenants store. Must be thread-safe.
type Tenants interface {
	List(ctx context.Context) ([]model.TenantInfo, error)

	New(ctx context.Context, tenant model.Tenant) error
	Read(ctx context.Context, name model.TenantName) (model.TenantInfo, error)
	Update(ctx context.Context, tenant model.Tenant, guard model.Version) error
	Delete(ctx context.Context, name model.TenantName) error
}

// Domains is a versioned domains store. Must be thread-safe.
type Domains interface {
	List(ctx context.Context) ([]model.Domain, error)

	New(ctx context.Context, domain model.Domain) error
	Read(ctx context.Context, name model.QualifiedDomainName) (model.Domain, error)
	Update(ctx context.Context, domain model.Domain, guard model.Version) error
	Delete(ctx context.Context, name model.QualifiedDomainName) error
}

// Placements is a versioned placement store. Must be thread-safe.
type Placements interface {
	List(ctx context.Context, tenant model.TenantName) ([]core.InternalPlacementInfo, error)

	Create(ctx context.Context, placement core.InternalPlacement) (core.InternalPlacementInfo, error)
	Read(ctx context.Context, name model.QualifiedPlacementName) (core.InternalPlacementInfo, error)
	Update(ctx context.Context, placement core.InternalPlacement, guard model.Version) (core.InternalPlacementInfo, error)
	Delete(ctx context.Context, name model.QualifiedPlacementName) error
}
