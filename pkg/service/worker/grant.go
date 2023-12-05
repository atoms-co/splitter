package worker

import (
	"go.atoms.co/lib/clockx"
	"go.atoms.co/splitter/pkg/core"
	"go.atoms.co/splitter/pkg/service/coordinator"
	"fmt"
)

// LeaseState represents the current lease state of a grant.
type LeaseState string

const (
	// LeaseActive represents that the grant is active and under the (renewed) lease.
	LeaseActive LeaseState = "active"
	// LeaseRevoked represents that the grant has been revoked by the leader. Lease is detached.
	LeaseRevoked LeaseState = "revoked"
	// LeaseStale represents that the grant is active, but will lapse due to a leader disconnect. Most
	// grants will go back to active, but that is the leader's decision.
	LeaseStale LeaseState = "stale"
)

// Grant holds a grant and its metadata and bookkeeping.
type Grant struct {
	Grant       core.Grant // holds original lease
	State       LeaseState
	Lease       *clockx.Timer
	Coordinator coordinator.Coordinator
	Updates     chan<- core.Update
}

func (g *Grant) ToUpdated() core.Grant {
	return core.NewGrant(g.Grant.ID(), g.Grant.Service(), g.Lease.Ttl(), g.Grant.Assigned())
}

func (g *Grant) String() string {
	return fmt.Sprintf("grant=%v, lease_state=%v, ttl=%v, coordinator=%v", g.Grant, g.State, g.Lease.Ttl().Unix(), g.Coordinator)
}
