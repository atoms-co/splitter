package storage

import (
	"context"
	"go.atoms.co/splitter/pkg/core"
	"go.atoms.co/splitter/pkg/model"
)

// Placements is a versioned placement store. Must be thread-safe.
type Placements interface {
	List(ctx context.Context, tenant model.TenantName) ([]core.InternalPlacementInfo, error)

	Create(ctx context.Context, placement core.InternalPlacement) (core.InternalPlacementInfo, error)
	Read(ctx context.Context, name model.QualifiedPlacementName) (core.InternalPlacementInfo, error)
	Update(ctx context.Context, placement core.InternalPlacement, guard model.Version) (core.InternalPlacementInfo, error)
	Delete(ctx context.Context, name model.QualifiedPlacementName) error
}
