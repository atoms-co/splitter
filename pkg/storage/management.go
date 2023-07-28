package storage

import (
	"context"
	"go.atoms.co/splitter/pkg/model"
)

// Management provide access to the management table. The table is expected to be small with all values cached
// in memory by the leader. Thread-safe.
type Management struct {
	Tenants    Tenants
	Placements Placements
}

type Tenants interface {
	// List returns the stored tenants
	List(ctx context.Context) ([]model.TenantInfo, error)

	// New records a new tenant.
	New(ctx context.Context, tenant model.Tenant) error

	// Read returns a stored tenant
	Read(ctx context.Context, name model.TenantName) (model.TenantInfo, error)

	// Update updates the tenant with the given name
	Update(ctx context.Context, tenant model.Tenant, guard UpdateGuard) error

	// Delete deletes the tenant with the given name
	Delete(ctx context.Context, name model.TenantName) error
}
