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
// Penalty rules must be "semi-deterministic" meaning that they must be deterministic for connected
// workers, but may change while a worker is disconnected.
type Allocation[T, D comparable] struct {
	id location.InstanceID

	place []Placement[T, D]
	colo  []Colocation[T, D]

	work    map[T]Work[T, D] // work data
	domains map[D]Load       // intrinsic load per domain
	load    Load             // total intrinsic load

	workers    map[WorkerID]*worker[T, D] // workers w/ active + allocated + revoked grants
	live       map[T]WorkerID             // worker lookup for active + allocated grants (but not revoked)
	inactive   map[T]Grant[T, D]          // inactive grants w/ _prior_ connected grant state
	unassigned map[T]time.Time            // unassigned with future allocation time
	seqno      int64                      // grant id seqno (for debuggability)
}

func New[T, D comparable](id location.InstanceID, place []Placement[T, D], colo []Colocation[T, D], work []Work[T, D], activation time.Time) *Allocation[T, D] {
	ret := &Allocation[T, D]{
		id:         id,
		place:      place,
		colo:       colo,
		work:       map[T]Work[T, D]{},
		workers:    map[WorkerID]*worker[T, D]{},
		live:       map[T]WorkerID{},
		inactive:   map[T]Grant[T, D]{},
		unassigned: map[T]time.Time{},
		domains:    map[D]Load{},
	}

	for _, w := range work {
		ret.work[w.Unit] = w
		ret.domains[w.Domain] += w.Load
		ret.load += w.Load
		ret.unassigned[w.Unit] = activation
	}

	return ret
}

func (a *Allocation[T, D]) ID() location.InstanceID {
	return a.id
}

func (a *Allocation[T, D]) Work() []Work[T, D] {
	return mapx.Values(a.work)
}

func (a *Allocation[T, D]) Workers() []Worker {
	return mapx.MapValues(a.workers, func(v *worker[T, D]) Worker {
		return v.info
	})
}

// Attach connects or re-connects a worker. If the latter, it includes a list of claimed existing grants.
// If the grants exist and are inactive, they are revived to their prior state. Additionally, if the
// grants are _unassigned_ they are activated to the claimed state to allow an allocation restart.
// Returns false if worker is already present.
func (a *Allocation[T, D]) Attach(info Worker, grants ...Grant[T, D]) ([]Grant[T, D], bool) {
	if _, ok := a.workers[info.ID]; ok {
		return nil, false
	}

	w := &worker[T, D]{
		info:    info,
		live:    map[T]live[T, D]{},
		revoked: map[T]Grant[T, D]{},
		place:   map[T]Load{},
	}
	a.workers[info.ID] = w

	var ret []Grant[T, D]
	for _, candidate := range grants {
		if g, ok := a.tryRevive(w, candidate); ok {
			ret = append(ret, g)
		}
	}
	w.load.Colo, w.colo = a.colocate(w.info, w.Live())
	return ret, true
}

func (a *Allocation[T, D]) tryRevive(w *worker[T, D], candidate Grant[T, D]) (Grant[T, D], bool) {
	if w.info.ID != candidate.Worker {
		return Grant[T, D]{}, false // skip: bad candidate
	}

	// Case 1: Worker was detached and the candidate is a now Inactive grant. Return the inactive grant
	// to ensure correct state. The candidate may be Active, but has been Revoked meanwhile.

	if grant, ok := a.inactive[candidate.Unit]; ok {
		if grant.ID != candidate.ID || grant.Worker != candidate.Worker {
			return Grant[T, D]{}, false // skip: claim is stale or conflicting
		}
		grant.Expiration = w.info.Lease

		if a.tryAssign(w, grant) {
			delete(a.inactive, candidate.Unit)
			return grant, true
		}
		return Grant[T, D]{}, false
	}

	// Case 2: Allocation is restarted or the inactive grant had been expired, but not yet reassigned. Only
	// accept Active candidates to avoid (temporarily) creating half of a transition -- which would violate
	// the grant state invariant. If candidate is Revoked, shorten the expiration to the grant.

	if _, ok := a.unassigned[candidate.Unit]; ok {
		switch candidate.State {
		case Active:
			candidate.Expiration = w.info.Lease

			if a.tryAssign(w, candidate) {
				delete(a.unassigned, candidate.Unit)
				return candidate, true
			}
			return Grant[T, D]{}, false

		case Revoked:
			a.unassigned[candidate.Unit] = candidate.Expiration
			return Grant[T, D]{}, false

		default:
			return Grant[T, D]{}, false
		}
	}

	// Case 3: Invalid work or already assigned.

	return Grant[T, D]{}, false
}

func (a *Allocation[T, D]) tryAssign(w *worker[T, D], g Grant[T, D]) bool {
	if g.Worker != w.info.ID {
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
		if load, ok := a.tryPlace(w.info, work); ok {
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

			a.live[work.Unit] = w.info.ID
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

// Detach removes the worker from consideration. Returns any outstanding now Inactive grants.
// Returns false if not found or already disconnected.
func (a *Allocation[T, D]) Detach(id WorkerID) ([]Grant[T, D], bool) {
	w, ok := a.workers[id]
	if !ok {
		return nil, false
	}

	var ret []Grant[T, D]
	for _, l := range w.live {
		g := w.ToGrant(l)
		delete(a.live, g.Unit)
		a.inactive[g.Unit] = g
		ret = append(ret, g.ToState(Inactive))
	}
	for _, g := range w.revoked {
		a.inactive[g.Unit] = g
		ret = append(ret, g.ToState(Inactive))
	}
	delete(a.workers, id)

	return ret, true
}

// Assigned returns the currently assigned grants -- incl revoked grants -- for an attached worker.
func (a *Allocation[T, D]) Assigned(id WorkerID) ([]Grant[T, D], bool) {
	if w, ok := a.workers[id]; ok {
		return append(mapx.MapValues(w.live, w.ToGrant), mapx.Values(w.revoked)...), true
	}
	return nil, false
}

// Extend extends the worker lease and thereby the expiration of any Active or Allocated grants.
// Returns the updated worker.
func (a *Allocation[T, D]) Extend(id WorkerID, expiration time.Time) (Worker, bool) {
	if w, ok := a.workers[id]; ok {
		w.info.Lease = expiration
		return w.info, true
	}
	return Worker{}, false
}

// Expire expires any revoked and inactive grants. If a Revoked grant is expired, it promotes the
// Allocated counterpart to Active (but implicitly so if Inactive). Returns promoted, attached grants.
func (a *Allocation[T, D]) Expire(now time.Time) []Grant[T, D] {
	var ret []Grant[T, D]

	for _, w := range a.workers {
		for t, g := range w.revoked {
			if now.Before(g.Expiration) {
				continue // skip: not expired
			}

			if promo, ok := a.tryPromote(g); ok {
				ret = append(ret, promo)
			}

			delete(w.revoked, t)
		}
	}

	for t, g := range a.inactive {
		if now.Before(g.Expiration) {
			continue // skip: not expired
		}

		switch g.State {
		case Allocated:
			// CAVEAT(herohde) 10/29/2023: the counterpart of a Revoked grant must not expire earlier than the
			// revoked grant itself. That would be a violation of the Revoked/Allocated invariant. If the worker
			// lease is short as this case indicates, we delay the expiration.
			continue

		case Revoked:
			if promo, ok := a.tryPromote(g); ok {
				ret = append(ret, promo)
			}
			continue

		default:
			// ok
		}

		delete(a.inactive, t)
		a.unassigned[t] = g.Expiration
	}
	return ret
}

// tryPromote promotes corresponding Allocated grant of the given Revoked grant. Promotion is silent, if detached.
func (a *Allocation[T, D]) tryPromote(revoked Grant[T, D]) (Grant[T, D], bool) {
	if revoked.State != Revoked {
		return Grant[T, D]{}, false
	}

	t := revoked.Unit

	if wid, ok := a.live[t]; ok {
		if w, ok := a.workers[wid]; ok {
			other := w.live[t]
			other.state = Active
			w.live[t] = other

			return w.ToGrant(other), true
		}
		return Grant[T, D]{}, false
	}

	if other, ok := a.inactive[t]; ok {
		other.State = Active
		a.inactive[t] = other
	}

	return Grant[T, D]{}, false
}

// Relinquish relinquishes a revoked grant. It completes a move and the returned grant promotes the Allocated
// destination grant to Assigned. Returns false if grant no longer valid.
func (a *Allocation[T, D]) Relinquish(id WorkerID, grant Grant[T, D], now time.Time) (Grant[T, D], bool) {
	panic("todo")
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

	workers := container.NewHeap[*worker[T, D]](func(a, b *worker[T, D]) bool { return a.Less(b) })
	for _, w := range a.workers {
		workers.Push(w)
	}

	var ret []Grant[T, D]

	for {
		next := assignable.Pop()

		var best *candidate[T, D]
		var buffer []*worker[T, D]

		// (a) Find best place for next. Assign early if possible.

		for workers.Len() > 0 {
			w := workers.Pop()
			buffer = append(buffer, w)

			load, ok := a.tryPlace(w.info, next.work)
			if !ok {
				continue
			}
			colo, m := a.colocate(w.info, w.LiveWith(next.work))

			cur := &candidate[T, D]{w: w, diff: AdjustedLoad{Load: next.work.Load, Place: load, Colo: colo - w.load.Colo}}
			if best == nil || cur.Less(best) {
				best = cur
			}

			zeroPenalty := cur.diff.Place == 0 && cur.diff.Colo <= 0 && m[next.work.Unit] == 0
			if zeroPenalty {
				break // assign immediately
			}
		}

		// (b) Assign work to the best candidate.

		if best != nil {
			work := next.work
			w := best.w

			fresh := NewGrant(a.newGrantID(), Active, work.Unit, work.Domain, w.info.ID, now, w.info.Lease)
			if a.tryAssign(w, fresh) {
				w.load.Colo, w.colo = a.colocate(w.info, w.Live())
				delete(a.unassigned, next.work.Unit)

				ret = append(ret, fresh)
			} // else internal: assignability validated above
		} // else skip: not assignable

		// (c) Return if done. Otherwise, restore worker heap for next round. The best candidate worker
		// will sort with the updated values for the next round.

		if assignable.Len() == 0 {
			return ret
		}

		for _, w := range buffer {
			workers.Push(w)
		}
	}
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

func (a *Allocation[T, D]) tryPlace(w Worker, work Work[T, D]) (Load, bool) {
	var ret Load
	for _, place := range a.place {
		load, ok := place.TryPlace(w, work)
		if !ok {
			return 0, false
		}
		ret += load
	}
	return ret, true
}

func (a *Allocation[T, D]) colocate(w Worker, work map[T]Work[T, D]) (Load, map[T]Load) {
	if len(work) == 0 {
		return 0, nil
	}

	var sum Load
	ret := map[T]Load{}
	for _, rule := range a.colo {
		for k, v := range rule.CoLocate(w, work) {
			sum += v
			ret[k] += v
		}
	}
	return sum, ret
}

func (a *Allocation[T, D]) newGrantID() GrantID {
	a.seqno++
	return GrantID(fmt.Sprintf("%v:%v", a.id, a.seqno))
}
