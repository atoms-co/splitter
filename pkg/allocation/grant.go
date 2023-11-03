package allocation

import (
	"fmt"
	"time"
)

// GrantID is a globally-unique fresh id for a grant to a specific worker.
type GrantID string

// GrantState describes the state of work assignment. A given work unit can have zero, one or two grants in
// different states at the same time. Work with no assignments has no grants. Note that an Allocated grant
// cannot be revoked, which would introduce a chain of grants for a work unit. The grant state is independent
// of whether the worker is attached or detached.
type GrantState string

const (
	// Active indicates work assigned to a single worker. No other grants for this work exist.
	Active GrantState = "active"
	// Allocated indicates the destination location for work movement, matching a Revoked grant. Grants are
	// directly created as active if not revoked from another worker.
	Allocated GrantState = "allocated"
	// Revoked indicates the source location for work movement, matching an Allocated or Inactive grant.
	Revoked GrantState = "revoked"
)

// Grant holds the state of work assignment to a worker.
type Grant[T, D comparable] struct {
	ID         GrantID
	State      GrantState
	Unit       T
	Domain     D
	Worker     WorkerID
	Assigned   time.Time
	Expiration time.Time
}

func NewGrant[T, D comparable](id GrantID, state GrantState, unit T, domain D, worker WorkerID, assigned, expiration time.Time) Grant[T, D] {
	return Grant[T, D]{
		ID:         id,
		State:      state,
		Unit:       unit,
		Domain:     domain,
		Worker:     worker,
		Assigned:   assigned,
		Expiration: expiration,
	}
}

func (g Grant[T, D]) IsActive() bool {
	return g.State == Active
}

func (g Grant[T, D]) IsAllocated() bool {
	return g.State == Allocated
}

func (g Grant[T, D]) IsRevoked() bool {
	return g.State == Revoked
}

func (g Grant[T, D]) WithState(state GrantState) Grant[T, D] {
	return NewGrant(g.ID, state, g.Unit, g.Domain, g.Worker, g.Assigned, g.Expiration)
}

func (g Grant[T, D]) WithExpiration(expiration time.Time) Grant[T, D] {
	return NewGrant(g.ID, g.State, g.Unit, g.Domain, g.Worker, g.Assigned, expiration)
}

func (g Grant[T, D]) String() string {
	return fmt.Sprintf("%v@%v[state=%v, unit=%v, domain=%v, assigned=%v, expiration=%v]", g.ID, g.Worker, g.State, g.Unit, g.Domain, g.Assigned, g.Expiration)
}

// Assignments holds assigned grants by status, i.e., steady, incoming and outgoing.
type Assignments[T, D comparable] struct {
	Active, Allocated, Revoked []Grant[T, D]
}

func (a Assignments[T, D]) IsEmpty() bool {
	return len(a.Active) == 0 && len(a.Allocated) == 0 && len(a.Revoked) == 0
}

func (a Assignments[T, D]) String() string {
	return fmt.Sprintf("{active=%v, allocated=%v, revoked=%v}", a.Active, a.Allocated, a.Revoked)
}

// Transition holds a grant movement with a Revoked from grant and an Allocated to grant.
type Transition[T, D comparable] struct {
	From, To Grant[T, D]
}

func (m Transition[T, D]) String() string {
	return fmt.Sprintf("%v -> %v", m.From, m.To)
}
