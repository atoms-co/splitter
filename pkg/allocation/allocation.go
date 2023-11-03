package allocation

import (
	"go.atoms.co/splitter/lib/service/location"
	"go.atoms.co/lib/container"
	"go.atoms.co/lib/mapx"
	"fmt"
	"time"
)

// Allocation assigns work of type T to a changeable workers under various constraint and preference
// rules. An allocation aim to minimize the adjusted total load of the work using a greedy algorithm.
// It may find a local optimum. After minimizing penalties, it attempts to evenly distribute the domain
// and total load.
//
// Penalty rules must be deterministic. If they change, a new allocation must be created and data
// transferred as possible. Update functions make the convenient.
type Allocation[T, D comparable] struct {
	id location.InstanceID

	place Placements[T, D]
	colo  Colocations[T, D]

	work    map[T]Work[T, D] // work data
	domains map[D]Load       // intrinsic load per domain
	load    Load             // total intrinsic load

	workers    map[WorkerID]*worker[T, D] // workers w/ assigned grants
	unassigned map[T]time.Time            // unassigned with future allocation time
	live       map[T]WorkerID             // worker index for active + allocated grants (but not revoked)
	seqno      int64                      // grant id seqno (for debuggability)
}

func New[T, D comparable](id location.InstanceID, place []Placement[T, D], colo []Colocation[T, D], work []Work[T, D], activation time.Time) *Allocation[T, D] {
	ret := &Allocation[T, D]{
		id:         id,
		place:      Placements[T, D]{List: place},
		colo:       Colocations[T, D]{List: colo},
		work:       map[T]Work[T, D]{},
		domains:    map[D]Load{},
		workers:    map[WorkerID]*worker[T, D]{},
		live:       map[T]WorkerID{},
		unassigned: map[T]time.Time{},
	}

	for _, w := range work {
		ret.work[w.Unit] = w
		ret.domains[w.Domain] += w.Load
		ret.load += w.Load
		ret.unassigned[w.Unit] = activation
	}

	return ret
}

// Update updates the work and allows re-evaluation of all rules, preserving existing assignments, expirations
// and state of workers as best possible. Returns the updated allocation and any prior unassignable or now
// invalid grants.
func Update[T, D comparable](a *Allocation[T, D], work []Work[T, D], activation time.Time) (*Allocation[T, D], []Grant[T, D]) {
	ret := New(a.id, a.place.List, a.colo.List, work, activation)
	ret.seqno = a.seqno

	// (1) Attach all workers and collect revokes. We can't simply Attach with grants, because transitional
	// grants will then be lost.

	revoked := map[T]Grant[T, D]{}
	for id, w := range a.workers {
		ret.workers[id] = newWorker[T, D](w.info.Instance, w.info.State, w.info.Lease)
		for k, v := range w.revoked {
			revoked[k] = v
		}
	}

	var unassignable []Grant[T, D]

	// (2) Try assign live work. If successful, assign the revoke as needed.

	for id, w := range ret.workers {
		old := a.workers[id]

		for t, l := range old.live {
			g := old.ToGrant(l)

			if _, ok := ret.work[t]; !ok {
				unassignable = append(unassignable, g)
				continue
			}

			if ret.tryAssign(w, g) {
				delete(ret.unassigned, g.Unit)

				if revoke, ok := revoked[g.Unit]; ok {
					_ = ret.tryAssign(ret.workers[revoke.Worker], revoke)
					delete(revoked, g.Unit)
				} // else: active
			} else {
				unassignable = append(unassignable, g)
			}
		}
	}

	// (3) Update expiration of unassigned work

	for t, expiration := range a.unassigned {
		if _, ok := ret.unassigned[t]; ok {
			ret.unassigned[t] = expiration
		}
	}

	return ret, append(unassignable, mapx.Values(revoked)...)
}

func (a *Allocation[T, D]) ID() location.InstanceID {
	return a.id
}

func (a *Allocation[T, D]) Work() []Work[T, D] {
	return mapx.Values(a.work)
}

func (a *Allocation[T, D]) Workers() []WorkerInfo {
	return mapx.MapValues(a.workers, func(w *worker[T, D]) WorkerInfo {
		return w.info
	})
}

func (a *Allocation[T, D]) Worker(id WorkerID) (WorkerInfo, bool) {
	if ret, ok := a.workers[id]; ok {
		return ret.info, true
	}
	return WorkerInfo{}, false
}

// Attach connects or re-connects a worker. If the latter, it includes a list of claimed existing grants.
// Any known existing grants exist are revived to their prior state. Additionally, if the allocation has no
// prior record of the worker, if the claimed grants are _unassigned_ and not in a transitional state, they
// are activated to the claimed state to allow an allocation restart.
//
// Returns assigned grants. Returns false if worker is already attached.
func (a *Allocation[T, D]) Attach(inst Worker, lease time.Time, grants ...Grant[T, D]) (Assignments[T, D], bool) {
	if w, ok := a.workers[inst.ID]; ok {
		if w.info.State != Detached {
			return Assignments[T, D]{}, false
		}

		// Case 1: Re-attach. Ignore claimed grants and restore prior known state. We don't re-compute
		// placements or loads as they are assumed to not change.

		w.info.State = Attached
		return w.Assignments(), true
	}

	// Case 2: Fresh attach. Revive claimed grants, if Active and unassigned.

	w := newWorker[T, D](inst, Attached, lease)
	a.workers[inst.ID] = w

	for _, g := range grants {
		if _, ok := a.unassigned[g.Unit]; ok && g.State == Active {
			g.Expiration = w.info.Lease
			if a.tryAssign(w, g) {
				delete(a.unassigned, g.Unit)
			} // else ignore: not assignable
		}
	}
	w.load.Colo, w.colo = a.colo.Colocate(w.info.Instance, w.Live())
	return w.Assignments(), true
}

func (a *Allocation[T, D]) tryAssign(w *worker[T, D], g Grant[T, D]) bool {
	if g.Worker != w.info.Instance.ID {
		return false // skip: incorrect worker for grant
	}
	if _, ok := w.live[g.Unit]; ok {
		return false // skip: already present
	}
	if _, ok := w.revoked[g.Unit]; ok {
		return false // skip: already present
	}

	switch g.State {
	case Active, Allocated:
		work := a.work[g.Unit]
		if load, ok := a.place.TryPlace(w.info.Instance, work); ok {
			w.live[g.Unit] = live[T, D]{
				work:     a.work[g.Unit],
				grant:    g.ID,
				state:    g.State,
				assigned: g.Assigned,
			}

			w.load.Load += work.Load
			if load > 0 {
				w.load.Place += load
				w.place[g.Unit] = load
				// colo is computed by the caller, once all assignments are made
			}

			a.live[work.Unit] = w.info.Instance.ID
			return true
		}
		return false

	case Revoked:
		// Do not check if placement is possible. No load impact.
		w.revoked[g.Unit] = g
		return true

	default:
		return false
	}
}

// Detach removes the worker from consideration. Returns false if not found or already detached.
func (a *Allocation[T, D]) Detach(id WorkerID) bool {
	if w, ok := a.workers[id]; ok {
		if w.info.State == Detached {
			return false
		}
		w.info.State = Detached
		return true
	}
	return false
}

// Assigned returns the currently assigned grants -- incl revoked grants -- for a worker.
func (a *Allocation[T, D]) Assigned(id WorkerID) Assignments[T, D] {
	if w, ok := a.workers[id]; ok {
		return w.Assignments()
	}
	return Assignments[T, D]{}
}

// Revoke revokes the given Active grants from the worker, if destination can be found. Returns
// transitions for the successful case as well as the rejected grants.
func (a *Allocation[T, D]) Revoke(id WorkerID, now time.Time, grants ...Grant[T, D]) ([]Transition[T, D], []Grant[T, D]) {
	w, ok := a.workers[id]
	if !ok {
		return nil, grants
	}

	// (1) Find other, attached workers. We cannot move work to the same worker.

	workers := newWorkerHeap[T, D]()
	for _, o := range a.workers {
		if o.info.State == Attached && o != w {
			workers.Push(o)
		}
	}
	if workers.Len() == 0 {
		return nil, grants // no other workers
	}

	// (2) Find best fit for moves, if any.

	var ret []Transition[T, D]
	var rejects []Grant[T, D]

	for _, grant := range grants {
		g, ok := w.live[grant.Unit]
		if !ok || g.state != Active {
			rejects = append(rejects, grant)
			continue
		}

		if dest, ok := a.tryAllocate(g.work, Allocated, workers, now); ok {
			// Update worker and re-compute load.

			revoked := w.ToGrant(g).WithState(Revoked)

			w.revoked[g.work.Unit] = revoked
			delete(w.live, g.work.Unit)

			w.load.Load -= g.work.Load
			w.load.Place -= w.place[g.work.Unit]
			delete(w.place, g.work.Unit)
			w.load.Colo, w.colo = a.colo.Colocate(w.info.Instance, w.Live())

			move := Transition[T, D]{
				From: revoked,
				To:   dest,
			}
			ret = append(ret, move)
		} else {
			rejects = append(rejects, grant)
		}
	}

	return ret, rejects
}

// Release releases a revoked grant. It completes a move and the returned grant promotes the
// Allocated destination grant to Assigned. Returns false if grant no longer valid.
func (a *Allocation[T, D]) Release(grant Grant[T, D]) (Grant[T, D], bool) {
	if w, ok := a.workers[grant.Worker]; ok {
		if g, ok := w.revoked[grant.Unit]; ok && g.ID == grant.ID {
			delete(w.revoked, grant.Unit)
			return a.tryPromote(g.Unit) // must exist
		}
	}
	return Grant[T, D]{}, false
}

// Unassign directly unassigns an active grant. Returns false if grant no longer valid.
func (a *Allocation[T, D]) Unassign(grant Grant[T, D], expiration time.Time) bool {
	t := grant.Unit

	if w, ok := a.workers[grant.Worker]; ok {
		if l, ok := w.live[t]; ok && l.state == Active && l.grant == grant.ID {
			delete(w.live, t)

			w.load.Load -= l.work.Load
			w.load.Place -= w.place[t]
			delete(w.place, t)
			w.load.Colo, w.colo = a.colo.Colocate(w.info.Instance, w.Live())

			a.unassigned[t] = expiration
			delete(a.live, t)

			return true
		}
	}
	return false
}

// Extend extends the worker lease and thereby the expiration of any Active or Allocated grants.
// Returns the updated worker. Returns false if not found or detached.
func (a *Allocation[T, D]) Extend(id WorkerID, expiration time.Time) (WorkerInfo, bool) {
	if w, ok := a.workers[id]; ok && w.info.State != Detached {
		w.info.Lease = expiration
		return w.info, true
	}
	return WorkerInfo{}, false
}

// Suspend marks the worker as suspended. Returns the updated worker. Returns false if not found
// or detached.
func (a *Allocation[T, D]) Suspend(id WorkerID) (WorkerInfo, bool) {
	if w, ok := a.workers[id]; ok && w.info.State != Detached {
		w.info.State = Suspended
		return w.info, true
	}
	return WorkerInfo{}, false
}

// Expire expires any revoked and inactive grants and workers. If a Revoked grant is expired, it promotes
// the Allocated counterpart to Active. Returns promoted grants.
func (a *Allocation[T, D]) Expire(now time.Time) []Grant[T, D] {
	var ret []Grant[T, D]

	for _, w := range a.workers {
		for t, g := range w.revoked {
			if now.Before(g.Expiration) {
				continue // skip: not expired
			}

			if promo, ok := a.tryPromote(g.Unit); ok {
				ret = append(ret, promo)
				delete(w.revoked, t)
			}
		}
	}

	for id, w := range a.workers {
		if w.info.State != Detached || now.Before(w.info.Lease) {
			continue // not a detached worker with expired lease
		}

		if len(w.live) > 0 {
			for t, l := range w.live {
				if l.state != Active {
					// CAVEAT(herohde) 10/29/2023: the counterpart of a Revoked grant must not expire earlier than the
					// revoked grant itself. That would be a violation of the Revoked/Allocated invariant. If the worker
					// lease is short as this case indicates, we delay the expiration. And why we expire revoked first.
					continue
				}

				w.load.Load -= l.work.Load
				w.load.Place -= w.place[l.work.Unit]
				delete(w.place, l.work.Unit)

				a.unassigned[t] = w.info.Lease
				delete(w.live, t)
				delete(a.live, t)
			}
			w.load.Colo, w.colo = a.colo.Colocate(w.info.Instance, w.Live())
		}

		if len(w.live) == 0 && len(w.revoked) == 0 {
			delete(a.workers, id)
		} // else: waiting for some revoked grants to expire
	}

	return ret
}

// tryPromote promotes corresponding Allocated grant of the given work unit. Returns false if not revoked.
func (a *Allocation[T, D]) tryPromote(t T) (Grant[T, D], bool) {
	if wid, ok := a.live[t]; ok {
		if w, ok := a.workers[wid]; ok && w.live[t].state == Allocated {
			other := w.live[t]
			other.state = Active
			w.live[t] = other

			return w.ToGrant(other), true
		}
	}
	return Grant[T, D]{}, false
}

type pending[T, D comparable] struct {
	work Work[T, D]
}

func (p pending[T, D]) Less(o pending[T, D]) bool {
	return p.work.Load > o.work.Load // highest intrinsic load
}

type candidate[T, D comparable] struct {
	w    *worker[T, D]
	diff AdjustedLoad
}

func (p *candidate[T, D]) Less(o *candidate[T, D]) bool {
	if a, b := p.diff.Total(), o.diff.Total(); a != b {
		return a < b
	}
	return p.w.Less(o.w)
}

// Allocate assigns all ready, unassigned work. It does not move or expire any assigned work. Returns
// new grants with worker lease expiration times.
func (a *Allocation[T, D]) Allocate(now time.Time) []Grant[T, D] {
	if len(a.workers) == 0 || len(a.unassigned) == 0 {
		return nil // no assignment possible
	}

	// (1) Find assignable work and determine placement order. For now, place high load work first.

	assignable := container.NewHeap[pending[T, D]](pending[T, D].Less)
	for t, activation := range a.unassigned {
		if now.Before(activation) {
			continue // skip: not ready
		}
		assignable.Push(pending[T, D]{work: a.work[t]})
	}

	if assignable.Len() == 0 {
		return nil // no assignable work
	}

	// (2) Order (likely) best worker by current adjusted load (assuming uniform penalty) and #work as tie-breaker.
	// Find the least penalty. If zero penalty then assign. This may miss a colocation affinity reduction for other
	// work, but will greatly speed up allocation in the normal case where penalties are sparse.

	workers := newWorkerHeap[T, D]()
	for _, w := range a.workers {
		if w.info.State == Attached {
			workers.Push(w)
		}
	}

	if workers.Len() == 0 {
		return nil // no eligible workers
	}

	var ret []Grant[T, D]
	for assignable.Len() > 0 {
		next := assignable.Pop()

		if g, ok := a.tryAllocate(next.work, Active, workers, now); ok {
			delete(a.unassigned, next.work.Unit)
			ret = append(ret, g)
		}
	}

	return ret
}

func (a *Allocation[T, D]) tryAllocate(work Work[T, D], state GrantState, workers *container.Heap[*worker[T, D]], now time.Time) (Grant[T, D], bool) {
	var best *candidate[T, D]
	var buffer []*worker[T, D]

	// (a) Find best place for next. Assign early if possible.

	for workers.Len() > 0 {
		w := workers.Pop()
		buffer = append(buffer, w)

		load, ok := a.place.TryPlace(w.info.Instance, work)
		if !ok {
			continue
		}
		colo, m := a.colo.Colocate(w.info.Instance, w.LiveWith(work))

		cur := &candidate[T, D]{w: w, diff: AdjustedLoad{Load: work.Load, Place: load, Colo: colo - w.load.Colo}}
		if best == nil || cur.Less(best) {
			best = cur
		}

		zeroPenalty := cur.diff.Place == 0 && cur.diff.Colo <= 0 && m[work.Unit] == 0
		if zeroPenalty {
			break // assign immediately
		}
	}

	// (b) Assign work to the best candidate.

	var ret *Grant[T, D]
	if best != nil {
		w := best.w

		fresh := NewGrant(a.newGrantID(), state, work.Unit, work.Domain, w.info.Instance.ID, now, w.info.Lease)
		if a.tryAssign(w, fresh) {
			w.load.Colo, w.colo = a.colo.Colocate(w.info.Instance, w.Live())
			ret = &fresh
		} // else internal: assignability validated above
	} // else skip: not assignable

	// (c) Restore worker heap. The best candidate worker will sort with the updated values for subsequent use.

	for _, w := range buffer {
		workers.Push(w)
	}

	if ret == nil {
		return Grant[T, D]{}, false
	}
	return *ret, true
}

func (a *Allocation[T, D]) LoadBalance(now time.Time) (Load, Action[T, D], bool) {
	// (1) consider work by placement penalty then co-location penalty in pq.
	// We can ignore work with zero penalty.

	// (2) consider swaps if no penalty- or skew-decreasing moves are possible

	return 0, Action[T, D]{}, false
}

// Load returns the total intrinsic load and current penalties by domain and rule.
func (a *Allocation[T, D]) Load() Load /* + more details */ {
	return a.load
}

func (a *Allocation[T, D]) String() string {
	return fmt.Sprintf("%v[#work=%v, #workers=%v, load=%v]", a.id, len(a.work), len(a.workers), a.load)
}

func (a *Allocation[T, D]) newGrantID() GrantID {
	a.seqno++
	return GrantID(fmt.Sprintf("%v:%v", a.id, a.seqno))
}
