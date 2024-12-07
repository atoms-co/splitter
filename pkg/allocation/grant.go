package allocation

import (
	"fmt"
	"time"
)

// GrantID is a globally-unique fresh id for a grant to a specific worker.
type GrantID string

// GrantState describes the state of work assignment. A given work unit can have zero, one or two grants in
// different states at the same time. Work with no assignments has no grants. The grant state is independent
// of whether the worker is attached or detached.
type GrantState string

const (
	// Active indicates work assigned to a single worker. No other grants for this work exist.
	Active GrantState = "active"
	// Allocated indicates the destination location for work movement, matching a Revoked grant (possibly
	// unassigned). Grants are directly created as active if not revoked from another worker.
	Allocated GrantState = "allocated"
	// Revoked indicates the source location for work movement, matching an Allocated grant (possibly unassigned).
	Revoked GrantState = "revoked"
)

// GrantModifier describes any modifications on the Grant. Currently two modifications as support, Loaded and Unloaded.
// TODO(jhhurwitz): 02/19/23 Consider making generic at some point
type GrantModifier string

const (
	// None indicates no modifier.
	None GrantModifier = ""
	// Loaded indicates the Grant has been Loaded by the worker of an Allocated Grant.
	Loaded GrantModifier = "loaded"
	// Unloaded indicates the Grant has been Unloaded by the worker of a Revoked Grant.
	Unloaded GrantModifier = "unloaded"
)

func (s GrantState) InvertIfTransitional() GrantState {
	switch s {
	case Revoked:
		return Allocated
	case Allocated:
		return Revoked
	default:
		return s
	}
}

// Grant holds the state of work assignment to a worker.
type Grant[T, K comparable] struct {
	ID         GrantID
	State      GrantState
	Mod        GrantModifier
	Unit       T
	Worker     K
	Assigned   time.Time
	Expiration time.Time
}

func NewGrant[T, K comparable](id GrantID, state GrantState, mod GrantModifier, unit T, worker K, assigned, expiration time.Time) Grant[T, K] {
	return Grant[T, K]{
		ID:         id,
		State:      state,
		Mod:        mod,
		Unit:       unit,
		Worker:     worker,
		Assigned:   assigned,
		Expiration: expiration,
	}
}

func (g Grant[T, K]) IsActive() bool {
	return g.State == Active
}

func (g Grant[T, K]) IsAllocated() bool {
	return g.State == Allocated
}

func (g Grant[T, K]) IsRevoked() bool {
	return g.State == Revoked
}

func (g Grant[T, K]) WithModifier(mod GrantModifier) Grant[T, K] {
	return NewGrant(g.ID, g.State, mod, g.Unit, g.Worker, g.Assigned, g.Expiration)
}

func (g Grant[T, K]) WithState(state GrantState) Grant[T, K] {
	return NewGrant(g.ID, state, None, g.Unit, g.Worker, g.Assigned, g.Expiration)
}

func (g Grant[T, K]) WithExpiration(expiration time.Time) Grant[T, K] {
	return NewGrant(g.ID, g.State, g.Mod, g.Unit, g.Worker, g.Assigned, expiration)
}

func (g Grant[T, K]) String() string {
	return fmt.Sprintf("Grant[id=%v, worker=%v, state=%v, unit=%v, assigned=%v, expiration=%v]", g.ID, g.Worker, g.State, g.Unit, g.Assigned.Unix(), g.Expiration.Unix())
}

// Assignments holds assigned grants by status, i.e., steady, incoming and outgoing.
type Assignments[T, K comparable] struct {
	Active, Allocated, Revoked []Grant[T, K]
}

func (a Assignments[T, K]) All() []Grant[T, K] {
	return append(a.Allocated, append(a.Revoked, a.Active...)...)
}

func (a Assignments[T, K]) IsEmpty() bool {
	return len(a.Active) == 0 && len(a.Allocated) == 0 && len(a.Revoked) == 0
}

func (a Assignments[T, K]) String() string {
	return fmt.Sprintf("{active=%v, allocated=%v, revoked=%v}", a.Active, a.Allocated, a.Revoked)
}
