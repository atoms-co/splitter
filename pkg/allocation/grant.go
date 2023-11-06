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
type Grant[T comparable] struct {
	ID         GrantID
	State      GrantState
	Unit       T
	Worker     WorkerID
	Assigned   time.Time
	Expiration time.Time
}

func NewGrant[T comparable](id GrantID, state GrantState, unit T, worker WorkerID, assigned, expiration time.Time) Grant[T] {
	return Grant[T]{
		ID:         id,
		State:      state,
		Unit:       unit,
		Worker:     worker,
		Assigned:   assigned,
		Expiration: expiration,
	}
}

func (g Grant[T]) IsActive() bool {
	return g.State == Active
}

func (g Grant[T]) IsAllocated() bool {
	return g.State == Allocated
}

func (g Grant[T]) IsRevoked() bool {
	return g.State == Revoked
}

func (g Grant[T]) WithState(state GrantState) Grant[T] {
	return NewGrant(g.ID, state, g.Unit, g.Worker, g.Assigned, g.Expiration)
}

func (g Grant[T]) WithExpiration(expiration time.Time) Grant[T] {
	return NewGrant(g.ID, g.State, g.Unit, g.Worker, g.Assigned, expiration)
}

func (g Grant[T]) String() string {
	return fmt.Sprintf("%v@%v[state=%v, unit=%v, assigned=%v, expiration=%v]", g.ID, g.Worker, g.State, g.Unit, g.Assigned.Unix(), g.Expiration.Unix())
}

// Assignments holds assigned grants by status, i.e., steady, incoming and outgoing.
type Assignments[T comparable] struct {
	Active, Allocated, Revoked []Grant[T]
}

func (a Assignments[T]) IsEmpty() bool {
	return len(a.Active) == 0 && len(a.Allocated) == 0 && len(a.Revoked) == 0
}

func (a Assignments[T]) String() string {
	return fmt.Sprintf("{active=%v, allocated=%v, revoked=%v}", a.Active, a.Allocated, a.Revoked)
}
