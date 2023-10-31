package worker

import (
	"go.atoms.co/splitter/pkg/core"
	"go.atoms.co/splitter/pkg/service/coordinator"
	"fmt"
)

// GrantState represents the current state of a grant.
type GrantState string

const (
	// GrantActive represents that the grant is active and under the (renewed) lease.
	GrantActive GrantState = "active"
	// GrantRevoked represents that the grant has been revoked by the leader. Lease is detached.
	GrantRevoked GrantState = "revoked"
	// GrantStale represents that the grant is active, but will lapse due to a leader disconnect. Most
	// grants will go back to active, but that is the leader's decision.
	GrantStale GrantState = "stale"
)

// Grant holds a grant and its metadata and bookkeeping.
type Grant struct {
	Grant       core.Grant // holds original lease
	State       GrantState
	Lease       Lease
	Coordinator coordinator.Coordinator
}

func (g *Grant) ToUpdated() core.Grant {
	return core.NewGrant(g.Grant.ID(), g.Grant.Tenant(), g.Lease.Expiration(), g.Grant.Assigned())
}

func (g *Grant) String() string {
	return fmt.Sprintf("grant=%v, state=%v, ttl=%v, coordinator=%v", g.Grant, g.State, g.Lease.Expiration().Unix(), g.Coordinator)
}
