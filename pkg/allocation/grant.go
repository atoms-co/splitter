package allocation

import (
	"go.atoms.co/lib/mapx"
	"fmt"
	"time"
)

// GrantID is a globally-unique fresh id for a grant to a specific worker.
type GrantID string

// GrantState describes the state of work assignment. A given work unit can have zero, one or two grants in
// different states at the same time. Work with no assignments has no grants. Note that an Allocated grant
// cannot be revoked, which would introduce a chain of grants for a work unit.
type GrantState string

const (
	// Active indicates work assigned to a single worker. No other grants for this work exist.
	Active GrantState = "active"
	// Allocated indicates the destination location for work movement, matching a Revoked grant. Grants are
	// directly created as active if not revoked from another worker.
	Allocated GrantState = "allocated"
	// Revoked indicates the source location for work movement, matching an Allocated or Inactive grant.
	Revoked GrantState = "revoked"
	// Inactive indicates an expiring grant with no current worker. It may be recaptured to an Active or
	// Allocated state if the worker re-connects.
	Inactive GrantState = "inactive"
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

func (g Grant[T, D]) Extend(expiration time.Time) Grant[T, D] {
	return NewGrant(g.ID, g.State, g.Unit, g.Domain, g.Worker, g.Assigned, expiration)
}

func (g Grant[T, D]) ToState(state GrantState) Grant[T, D] {
	return NewGrant(g.ID, state, g.Unit, g.Domain, g.Worker, g.Assigned, g.Expiration)
}

func (g Grant[T, D]) String() string {
	return fmt.Sprintf("%v@%v[state=%v, unit=%v, domain=%v, assigned=%v, expiration=%v]", g.ID, g.Worker, g.State, g.Unit, g.Domain, g.Assigned, g.Expiration)
}

func NewGrantMap[T, D comparable](list ...Grant[T, D]) map[T]Grant[T, D] {
	return mapx.New(list, func(v Grant[T, D]) T {
		return v.Unit
	})
}

// Transition holds a grant movement with a Revoked from grant and an Allocated to grant.
type Transition[T, D comparable] struct {
	From, To Grant[T, D]
}

func NewTransition[T, D comparable](g Grant[T, D], id GrantID, to WorkerID, now, expiration time.Time) (Transition[T, D], bool) {
	if g.State != Active || g.Worker == to {
		return Transition[T, D]{}, false // not valid
	}

	return Transition[T, D]{
		From: g.ToState(Revoked),
		To:   NewGrant(id, Allocated, g.Unit, g.Domain, to, now, expiration),
	}, true
}

func (m Transition[T, D]) String() string {
	return fmt.Sprintf("%v -> %v", m.From, m.To)
}
