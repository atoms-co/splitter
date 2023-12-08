package allocation

import (
	"go.atoms.co/splitter/lib/service/location"
	"go.atoms.co/lib/container"
	"go.atoms.co/lib/mapx"
	"go.atoms.co/lib/mathx"
	"fmt"
	"time"
)

// unassigned holds unassigned work or grant in any state. For revoked grants, the activation time is
// the promotion time for the corresponding Allocated grant. If both legs of a transition ends up
// unassigned, they combine to Active w/ the expiration time of the revoke.
type unassigned struct {
	state      GrantState
	activation time.Time
}

func newUnassigned(state GrantState, activation time.Time) unassigned {
	return unassigned{state: state, activation: activation}
}

func (u unassigned) String() string {
	return fmt.Sprintf("%v@%v", u.state, u.activation.Unix())
}

// Allocation assigns work of type T to a changeable workers under various constraint and preference
// rules. An allocation aim to minimize the adjusted total load of the work using a greedy algorithm.
// It may find a local optimum. After minimizing penalties, it attempts to evenly distribute the domain
// and total load.
//
// Penalty rules must be deterministic. If they change, a new allocation must be created and data
// transferred as possible. Update functions make the convenient.
type Allocation[T comparable, W any, K comparable, V any] struct {
	id location.InstanceID

	place Placements[T, W, K, V]
	colo  Colocations[T, W, K, V]

	work map[T]Work[T, W] // work data
	load Load             // total intrinsic load

	workers    map[K]*worker[T, W, K, V] // workers w/ assigned grants
	unassigned map[T]unassigned          // unassigned work with future activation time
	live       map[T]K                   // worker index for live (active + allocated) grants
	seqno      int64                     // grant id seqno (for debuggability)
}

func New[T comparable, W any, K comparable, V any](id location.InstanceID, place []Placement[T, W, K, V], colo []Colocation[T, W, K, V], work []Work[T, W], activation time.Time) *Allocation[T, W, K, V] {
	ret := &Allocation[T, W, K, V]{
		id:         id,
		place:      Placements[T, W, K, V]{List: place},
		colo:       Colocations[T, W, K, V]{List: colo},
		work:       map[T]Work[T, W]{},
		workers:    map[K]*worker[T, W, K, V]{},
		live:       map[T]K{},
		unassigned: map[T]unassigned{},
	}

	for _, w := range work {
		w.Load = mathx.Max(w.Load, 1)

		ret.work[w.Unit] = w
		ret.load += w.Load
		ret.unassigned[w.Unit] = newUnassigned(Active, activation)
	}

	return ret
}

// Update updates the work and allows re-evaluation of all rules, preserving existing assignments, expirations
// and state of workers as best possible. Returns the updated allocation and any prior unassignable or now
// invalid grants.
func Update[T comparable, W any, K comparable, V any](a *Allocation[T, W, K, V], place []Placement[T, W, K, V], colo []Colocation[T, W, K, V], work []Work[T, W], activation time.Time) (*Allocation[T, W, K, V], []Grant[T, K]) {
	ret := New(a.id, place, colo, work, activation)
	ret.seqno = a.seqno

	// (1) Attach all workers and try to assign all old grants. That will preserve what grants can be preserved.

	var bad []Grant[T, K]

	for id, old := range a.workers {
		w := newWorker[T, W, K, V](old.info.Instance, old.info.State, old.info.Lease)
		ret.workers[id] = w

		for _, l := range old.live {
			g := old.ToGrant(l)
			if ret.tryAssign(w, g) {
				u := ret.unassigned[g.Unit]
				if g.IsAllocated() && u.state == Active {
					ret.unassigned[g.Unit] = newUnassigned(Revoked, u.activation) // other half
				} else {
					delete(ret.unassigned, g.Unit)
				}
			} else {
				bad = append(bad, g)
			}
		}

		for _, g := range old.revoked {
			if ret.tryAssign(w, g) {
				u := ret.unassigned[g.Unit]
				if u.state == Active {
					ret.unassigned[g.Unit] = newUnassigned(Allocated, u.activation) // other half
				} else {
					delete(ret.unassigned, g.Unit)
				}
			} else {
				bad = append(bad, g)
			}
		}

		w.load.Colo, w.colo = ret.colo.Colocate(w.info.Instance, w.Live())
	}

	// (2) Preserve expiration of unassigned work

	for t, old := range a.unassigned {
		if u, ok := ret.unassigned[t]; ok && old.state == u.state {
			ret.unassigned[t] = newUnassigned(u.state, old.activation)
		}
	}

	return ret, bad
}

// Reset clears all work in the given allocation. It returns an updated allocation with no work and
// all currents grants rejected.
func Reset[T comparable, W any, K comparable, V any](a *Allocation[T, W, K, V]) (*Allocation[T, W, K, V], []Grant[T, K]) {
	return Update(a, a.place.List, a.colo.List, nil, time.Now())
}

func (a *Allocation[T, W, K, V]) ID() location.InstanceID {
	return a.id
}

func (a *Allocation[T, W, K, V]) Work() []Work[T, W] {
	return mapx.Values(a.work)
}

func (a *Allocation[T, W, K, V]) Unit(t T) (Work[T, W], bool) {
	ret, ok := a.work[t]
	return ret, ok
}

func (a *Allocation[T, W, K, V]) Workers() []WorkerInfo[K, V] {
	return mapx.MapValues(a.workers, func(w *worker[T, W, K, V]) WorkerInfo[K, V] {
		return w.info
	})
}

func (a *Allocation[T, W, K, V]) Worker(id K) (WorkerInfo[K, V], bool) {
	if ret, ok := a.workers[id]; ok {
		return ret.info, true
	}
	return WorkerInfo[K, V]{}, false
}

// Attach connects or re-connects a worker. If the latter, it includes a list of claimed existing grants.
// Any known existing grants exist are revived to their prior state. Additionally, if the allocation has no
// prior record of the worker, if the claimed grants are _unassigned_, they are activated regardless of
// expiration  to the claimed state to allow an allocation restart.
//
// Returns assigned grants. Returns false if worker is already attached.
func (a *Allocation[T, W, K, V]) Attach(inst Worker[K, V], lease time.Time, grants ...Grant[T, K]) (Assignments[T, K], bool) {
	if w, ok := a.workers[inst.ID]; ok {
		if w.info.State != Detached {
			return Assignments[T, K]{}, false
		}

		// Case 1: Re-attach. Ignore claimed grants and restore prior known state. We don't re-compute
		// placements or loads as they are assumed to not change.

		w.info.State = Attached
		return w.Assignments(), true
	}

	// Case 2: Fresh attach. Revive claimed grants, if unassigned.

	w := newWorker[T, W, K, V](inst, Attached, lease)
	a.workers[inst.ID] = w

	for _, g := range grants {
		if u, ok := a.unassigned[g.Unit]; ok && (u.state == Active || g.State == u.state) {
			if !g.IsRevoked() {
				g.Expiration = w.info.Lease
			}
			if !a.tryAssign(w, g) {
				continue // ignore: not assignable
			}

			if u.state == Active && !g.IsActive() {
				a.unassigned[g.Unit] = newUnassigned(g.State.InvertIfTransitional(), u.activation) // other half
			} else {
				delete(a.unassigned, g.Unit)
			}
		} // else ignore: stale grant
	}
	w.load.Colo, w.colo = a.colo.Colocate(w.info.Instance, w.Live())
	return w.Assignments(), true
}

func (a *Allocation[T, W, K, V]) tryAssign(w *worker[T, W, K, V], g Grant[T, K]) bool {
	if g.Worker != w.info.Instance.ID {
		return false // skip: incorrect worker for grant
	}
	if _, ok := w.live[g.Unit]; ok {
		return false // skip: already assigned
	}
	if _, ok := w.revoked[g.Unit]; ok {
		return false // skip: already present
	}

	work, ok := a.work[g.Unit]
	if !ok {
		return false // skip: invalid
	}

	switch g.State {
	case Active, Allocated:
		if _, ok := a.live[g.Unit]; ok {
			return false // skip: already present elsewhere
		}

		if load, ok := a.place.TryPlace(w.info.Instance, work); ok {
			w.live[g.Unit] = live[T, W]{
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

// Suspend marks the worker as suspended. Returns the updated worker. Returns false if not found
// or detached.
func (a *Allocation[T, W, K, V]) Suspend(id K) (WorkerInfo[K, V], bool) {
	if w, ok := a.workers[id]; ok && w.info.State != Detached {
		w.info.State = Suspended
		return w.info, true
	}
	return WorkerInfo[K, V]{}, false
}

// Detach removes the worker from consideration. Returns false if not found or already detached.
func (a *Allocation[T, W, K, V]) Detach(id K) bool {
	if w, ok := a.workers[id]; ok {
		if w.info.State == Detached {
			return false
		}
		w.info.State = Detached
		return true
	}
	return false
}

// Remove removes a worker from the allocation if it has no grants. Returns false if not found or not removable.
func (a *Allocation[T, W, K, V]) Remove(id K) bool {
	if w, ok := a.workers[id]; ok {
		if len(w.live) == 0 && len(w.revoked) == 0 {
			delete(a.workers, id)
			return true
		}
		return false
	}
	return false
}

// Assigned returns the currently assigned grants -- incl revoked grants -- for a worker.
func (a *Allocation[T, W, K, V]) Assigned(id K) Assignments[T, K] {
	if w, ok := a.workers[id]; ok {
		return w.Assignments()
	}
	return Assignments[T, K]{}
}

// Revoke revokes the given Active grants from the worker. The counterparts become immediately assignable.
// Returns successfully revoked grants. Returns false if worker not present.
func (a *Allocation[T, W, K, V]) Revoke(id K, now time.Time, grants ...Grant[T, K]) ([]Grant[T, K], bool) {
	w, ok := a.workers[id]
	if !ok {
		return nil, false
	}

	var ret []Grant[T, K]
	for _, grant := range grants {
		t := grant.Unit

		g, ok := w.live[t]
		if !ok || g.grant != grant.ID || g.state != Active {
			continue
		}

		revoked := a.revoke(w, now, g)
		ret = append(ret, revoked)
	}

	return ret, true
}

func (a *Allocation[T, W, K, V]) revoke(w *worker[T, W, K, V], now time.Time, g live[T, W]) Grant[T, K] {
	t := g.work.Unit

	// (1) Update worker and re-compute load.

	revoked := w.ToGrant(g).WithState(Revoked)

	w.revoked[t] = revoked
	delete(w.live, t)
	delete(a.live, t)

	w.load.Load -= g.work.Load
	w.load.Place -= w.place[t]
	delete(w.place, t)
	w.load.Colo, w.colo = a.colo.Colocate(w.info.Instance, w.Live())

	// (2) Add counterpart to unassigned

	a.unassigned[t] = newUnassigned(Allocated, now)

	return revoked
}

// Release releases a grant in any state. If Revoked, it may complete a move with the returned grant promoting
// the Allocated destination grant to Active. Returns false if no promotion or if grant is no longer valid.
func (a *Allocation[T, W, K, V]) Release(grant Grant[T, K], now time.Time) (Grant[T, K], bool) {
	t := grant.Unit

	if w, ok := a.workers[grant.Worker]; ok {
		if g, ok := w.revoked[t]; ok && g.ID == grant.ID {
			delete(w.revoked, t)
			return a.tryPromote(t, now)
		}
		if l, ok := w.live[t]; ok && l.grant == grant.ID {
			delete(w.live, t)
			delete(a.live, t)

			w.load.Load -= l.work.Load
			w.load.Place -= w.place[t]
			delete(w.place, t)
			w.load.Colo, w.colo = a.colo.Colocate(w.info.Instance, w.Live())

			if u, ok := a.unassigned[t]; ok {
				a.unassigned[t] = newUnassigned(Active, u.activation) // combine with unassigned Revoke
			} else {
				a.unassigned[t] = newUnassigned(l.state, w.info.Lease)
			}
		}
	}
	return Grant[T, K]{}, false
}

// Extend extends the worker lease and thereby the expiration of any Active or Allocated grants.
// Returns the updated worker. Returns false if not found or detached.
func (a *Allocation[T, W, K, V]) Extend(id K, expiration time.Time) (WorkerInfo[K, V], bool) {
	if w, ok := a.workers[id]; ok && w.info.State != Detached {
		w.info.Lease = expiration
		return w.info, true
	}
	return WorkerInfo[K, V]{}, false
}

// Expire expires any revoked and inactive grants and workers. If a Revoked grant or unassigned work expires,
// it promotes the Allocated counterpart to Active. Returns promoted grants.
func (a *Allocation[T, W, K, V]) Expire(now time.Time) []Grant[T, K] {
	var ret []Grant[T, K]

	for _, w := range a.workers {
		for t, g := range w.revoked {
			if now.Before(g.Expiration) {
				continue // skip: not expired
			}

			if promo, ok := a.tryPromote(g.Unit, now); ok {
				ret = append(ret, promo)
			}
			delete(w.revoked, t)
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

				delete(w.live, t)
				delete(a.live, t)

				w.load.Load -= l.work.Load
				w.load.Place -= w.place[l.work.Unit]
				delete(w.place, l.work.Unit)

				a.unassigned[t] = newUnassigned(Active, w.info.Lease)
			}
			w.load.Colo, w.colo = a.colo.Colocate(w.info.Instance, w.Live())
		}

		if len(w.live) == 0 && len(w.revoked) == 0 {
			delete(a.workers, id)
		} // else: waiting for some revoked grants to expire
	}

	return ret
}

// tryPromote promotes corresponding Allocated grant of the given work unit. Returns the promotion grant
// if present.
func (a *Allocation[T, W, K, V]) tryPromote(t T, now time.Time) (Grant[T, K], bool) {
	if wid, ok := a.live[t]; ok {
		if w, ok := a.workers[wid]; ok && w.live[t].state == Allocated {
			other := w.live[t]
			other.state = Active
			w.live[t] = other

			return w.ToGrant(other), true
		}
		return Grant[T, K]{}, false
	}

	if u, ok := a.unassigned[t]; ok && u.state == Allocated {
		a.unassigned[t] = newUnassigned(Active, now)
	}

	return Grant[T, K]{}, false
}

type pending[T comparable, W any, K comparable, V any] struct {
	work  Work[T, W]
	state GrantState
}

func (p pending[T, W, K, V]) Less(o pending[T, W, K, V]) bool {
	return p.work.Load > o.work.Load // highest intrinsic load
}

type candidate[T comparable, W any, K comparable, V any] struct {
	w    *worker[T, W, K, V]
	diff AdjustedLoad
}

func (p *candidate[T, W, K, V]) Less(o *candidate[T, W, K, V]) bool {
	if a, b := p.diff.Total(), o.diff.Total(); a != b {
		return a < b
	}
	return p.w.Less(o.w)
}

// Allocate assigns all ready, unassigned work. It does not move or expire any assigned work. Returns
// new grants with worker lease expiration times.
func (a *Allocation[T, W, K, V]) Allocate(now time.Time) []Grant[T, K] {
	if len(a.workers) == 0 || len(a.unassigned) == 0 {
		return nil // no assignment possible
	}

	var ret []Grant[T, K]

	// (1) Find assignable work and determine placement order. For now, place high load work first.

	assignable := container.NewHeap[pending[T, W, K, V]](pending[T, W, K, V].Less)
	for t, u := range a.unassigned {
		if now.Before(u.activation) {
			continue // skip: not ready
		}

		if u.state == Revoked {
			// Unassigned revoked work yields a promotion, not a new assignment.

			if g, ok := a.tryPromote(t, now); ok {
				ret = append(ret, g)
			}
			delete(a.unassigned, t)
			continue
		}

		assignable.Push(pending[T, W, K, V]{work: a.work[t], state: u.state})
	}

	if assignable.Len() == 0 {
		return ret // no assignable work
	}

	// (2) Order (likely) best worker by current adjusted load (assuming uniform penalty) and #work as tie-breaker.
	// Find the least penalty. If zero penalty then assign. This may miss a colocation affinity reduction for other
	// work, but will greatly speed up allocation in the normal case where penalties are sparse.

	workers := newWorkerHeap[T, W, K, V]()
	for _, w := range a.workers {
		if w.info.State == Attached {
			workers.Push(w)
		}
	}

	if workers.Len() == 0 {
		return ret // no eligible workers
	}

	for assignable.Len() > 0 {
		next := assignable.Pop()

		if g, ok := a.tryAllocate(next.work, next.state, workers, now); ok {
			delete(a.unassigned, next.work.Unit)
			ret = append(ret, g)
		}
	}

	return ret
}

func (a *Allocation[T, W, K, V]) tryAllocate(work Work[T, W], state GrantState, workers *container.Heap[*worker[T, W, K, V]], now time.Time) (Grant[T, K], bool) {
	var best *candidate[T, W, K, V]
	var buffer []*worker[T, W, K, V]

	// (a) Find best place for next. Assign early if possible.

	for workers.Len() > 0 {
		w := workers.Pop()
		buffer = append(buffer, w)

		if _, ok := w.Grant(work.Unit); ok {
			continue // skip: holds other half of transitional grant
		}
		load, ok := a.place.TryPlace(w.info.Instance, work)
		if !ok {
			continue // skip: invalid placement
		}
		colo, m := a.colo.Colocate(w.info.Instance, w.LiveWith(work))

		cur := &candidate[T, W, K, V]{w: w, diff: AdjustedLoad{Load: work.Load, Place: load, Colo: colo - w.load.Colo}}
		if best == nil || cur.Less(best) {
			best = cur
		}

		zeroPenalty := cur.diff.Place == 0 && cur.diff.Colo <= 0 && m[work.Unit] == 0
		if zeroPenalty {
			break // assign immediately
		}
	}

	// (b) Assign work to the best candidate.

	var ret *Grant[T, K]
	if best != nil {
		w := best.w

		fresh := NewGrant(a.newGrantID(), state, work.Unit, w.info.Instance.ID, now, w.info.Lease)
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
		return Grant[T, K]{}, false
	}
	return *ret, true
}

type penalized[T comparable, W any, K comparable, V any] struct {
	w     *worker[T, W, K, V]
	grant live[T, W]
	load  AdjustedLoad
}

func (p penalized[T, W, K, V]) Less(o penalized[T, W, K, V]) bool {
	// Sort penalized work by penalty first then overload needs:
	//
	//  (1) highest current placement penalty
	//  (2) highest current colocation penalty
	//  (3) highest current overload skew penalty
	//  (4) highest worker load
	//  (5) highest number of live grants (tiebreaker if intrinsic load is zero)

	if p.load.Place != o.load.Place {
		return p.load.Place > o.load.Place
	}
	if p.load.Colo != o.load.Colo {
		return p.load.Colo > o.load.Colo
	}
	if p.load.Load != o.load.Load {
		return p.load.Load > o.load.Load
	}
	if p.w.load.Load != o.w.load.Load {
		return p.w.load.Load > o.w.load.Load
	}
	return len(p.w.live) > len(o.w.live)
}

// TODO(herohde) 11/6/2023: maybe add a work filter to select which work is eligible. For
// example, avoid recently assigned work, specific domains or similar. It would be an addition
// placement rule?

// TODO(herohde) 11/5/2023: how aggressively should we even laod? Stability is also desirable.
// OTOH we also want upscaling to help. Maybe best done as a separate function?

// LoadBalance attempts to make a move that improves total adjusted load, or worker load balance as
// a secondary concern. Returns the move and load improvement, if successful.
func (a *Allocation[T, W, K, V]) LoadBalance(now time.Time) (Move[T, K], AdjustedLoad, bool) {

	// (1) Find attached workers and compute effective load skew. Ignore disconnected and
	// suspended workers. We allow the average unit load as slack.

	workers := mapx.ValuesIf(a.workers, func(w *worker[T, W, K, V]) bool {
		return w.info.State == Attached // skip: don't touch non-attached workers, even as a source
	})
	if len(workers) == 0 || len(a.work) == 0 {
		return Move[T, K]{}, AdjustedLoad{}, false
	}

	slack := Load(mathx.CeilDivInt(int(a.load), len(a.work)))
	expected := Load(mathx.CeilDivInt(int(a.load), len(workers)))
	cutoff := expected + slack

	// (2) Find largest penalty reductions, incl. effective load skew penalty. Consider active work
	// by placement penalty then co-location penalty in pq. Ignore work with zero penalties.

	movable := container.NewHeap[penalized[T, W, K, V]](penalized[T, W, K, V].Less)

	overload := map[K]Load{}
	for _, w := range workers {
		overload[w.info.Instance.ID] = mathx.Max(w.load.Load-cutoff, 0)

		for t, l := range w.live {
			if l.state != Active {
				continue // skip: cannot revoke Allocated grant
			}

			load := AdjustedLoad{
				Load:  (overload[w.info.Instance.ID] * l.work.Load) / w.load.Load,
				Place: w.place[t],
				Colo:  w.colo[t],
			}

			if load.Total() > 0 {
				movable.Push(penalized[T, W, K, V]{w: w, grant: l, load: load})
			} // else skip: fine where it is
		}
	}

	if movable.Len() == 0 || len(workers) < 2 {
		return Move[T, K]{}, AdjustedLoad{}, false
	}

	// (2) Evaluate a move to the least-loaded worker possible. Note that other work on that worker may
	// see a penalty increase if they favor affinity with the candidate work, so the actual gain may be
	// different from what is attributed to the candidate. It may even be a loss for the worker.
	//
	// However, any move an improvement to total adjusted load.

	for movable.Len() > 0 {
		next := movable.Pop()
		work := next.grant.work

		src, _ := a.colo.Colocate(next.w.info.Instance, next.w.LiveWithout(work.Unit))
		bonus := next.w.load.Colo - src // co-location penalty bonus at source (may be negative)

		// (a) Find penalty-improving move, if any.

		var best *candidate[T, W, K, V]
		for _, w := range workers {
			if w == next.w {
				continue // skip: self-move
			}

			intrin := w.load.Load + work.Load
			place, ok := a.place.TryPlace(w.info.Instance, work)
			if !ok {
				continue // skip: invalid placement
			}
			colo, _ := a.colo.Colocate(w.info.Instance, w.LiveWith(work))

			diff := AdjustedLoad{
				Load:  (mathx.Max(intrin-cutoff, 0)*work.Load)/intrin - next.load.Load,
				Place: place - next.load.Place,
				Colo:  colo - w.load.Colo - bonus,
			}

			if diff.Total() >= 0 {
				continue // skip: no penalty reduction
			}

			cur := &candidate[T, W, K, V]{w: w, diff: diff}
			if best == nil || cur.Less(best) {
				best = cur
			}
		}

		// (b) Perform move if found

		if best != nil {
			return a.move(next.w, next.grant, best.w, now), best.diff, true
		}
	}

	return Move[T, K]{}, AdjustedLoad{}, false
}

func (a *Allocation[T, W, K, V]) move(from *worker[T, W, K, V], grant live[T, W], to *worker[T, W, K, V], now time.Time) Move[T, K] {
	work := grant.work

	revoked := a.revoke(from, now, grant)
	allocated := NewGrant(a.newGrantID(), Allocated, work.Unit, to.info.Instance.ID, now, to.info.Lease)
	_ = a.tryAssign(to, allocated)

	return Move[T, K]{From: revoked, To: allocated}
}

// Load returns the total intrinsic load and current assignments/penalties.
func (a *Allocation[T, W, K, V]) Load() (Load, AdjustedLoad) {
	ret := AdjustedLoad{}
	for _, w := range a.workers {
		ret.Load += w.load.Load
		ret.Place += w.load.Place
		ret.Colo += w.load.Colo
	}
	return a.load, ret
}

// Size returns the number of work units.
func (a *Allocation[T, W, K, V]) Size() int {
	return len(a.work)
}

func (a *Allocation[T, W, K, V]) String() string {
	return fmt.Sprintf("%v[#work=%v, #workers=%v, load=%v]", a.id, len(a.work), len(a.workers), a.load)
}

func (a *Allocation[T, W, K, V]) newGrantID() GrantID {
	a.seqno++
	return GrantID(fmt.Sprintf("%v:%v", a.id, a.seqno))
}

// Check performs an internal consistency check. Debug only.
func (a *Allocation[T, W, K, V]) Check() error {
	// (1) Check grant integrity

	grants := map[GrantState]map[T]Grant[T, K]{
		Active:    {},
		Allocated: {},
		Revoked:   {},
	}

	for _, w := range a.workers {
		for _, l := range w.live {
			grants[l.state][l.work.Unit] = w.ToGrant(l)
			if a.live[l.work.Unit] != w.info.Instance.ID {
				return fmt.Errorf("missing grant index: %v", w.ToGrant(l))
			}
			if _, ok := a.work[l.work.Unit]; !ok {
				return fmt.Errorf("invalid work: %v", w.ToGrant(l))
			}
		}
		for _, g := range w.revoked {
			grants[g.State][g.Unit] = g
			if a.live[g.Unit] == w.info.Instance.ID {
				return fmt.Errorf("invalid revoked grant index: %v", g)
			}
			if _, ok := a.work[g.Unit]; !ok {
				return fmt.Errorf("invalid work: %v", g)
			}
		}
	}

	for t, g := range grants[Active] {
		if bad, ok := grants[Allocated][t]; ok {
			return fmt.Errorf("conflicting grants: %v <> %v", g, bad)
		}
		if bad, ok := grants[Revoked][t]; ok {
			return fmt.Errorf("conflicting grants: %v <> %v", g, bad)
		}
		if bad, ok := a.unassigned[t]; ok {
			return fmt.Errorf("bad unassigned: %v <> %v", g, bad)
		}
	}
	for t, g := range grants[Allocated] {
		if _, ok := grants[Revoked][t]; !ok {
			u, ok := a.unassigned[t]
			if !ok {
				return fmt.Errorf("missing counterpart grant: %v", g)
			}
			if u.state != Revoked {
				return fmt.Errorf("bad unassigned counterpart: %v <> %v", g, u)
			}
		}
	}
	for t, g := range grants[Revoked] {
		if _, ok := grants[Allocated][t]; !ok {
			u, ok := a.unassigned[t]
			if !ok {
				return fmt.Errorf("missing counterpart: %v", g)
			}
			if u.state != Allocated {
				return fmt.Errorf("bad unassigned counterpart: %v <> %v", g, u)
			}
		}
	}

	// (2) Check incremental load

	for _, w := range a.workers {
		var load AdjustedLoad
		for _, l := range w.live {
			load.Load += l.work.Load
		}
		for _, p := range w.place {
			load.Place += p
		}
		for _, c := range w.colo {
			load.Colo += c
		}
		if load != w.load {
			return fmt.Errorf("inconsistent load for %v: %v <> %v", w.info, w.load, load)
		}
	}

	// (3) Ensure all work is accounted for

	for t := range a.work {
		_, ok := a.live[t]
		_, ok2 := a.unassigned[t]
		if !ok && !ok2 {
			return fmt.Errorf("work neither live nor unassigned: %v", t)
		}
	}

	return nil
}
