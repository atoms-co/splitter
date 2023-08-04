package leader

import (
	"go.atoms.co/splitter/pkg/model"
	"time"
)

// GrantID is an id created at time of allocation.
type GrantID string

// Grant represents a granted tenant with an implicit lease.
type Grant struct {
	ID         GrantID
	Tenant     model.TenantName
	Expiration time.Time
}
