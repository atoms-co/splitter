package allocation

import (
	"go.atoms.co/splitter/lib/service/location"
	"go.atoms.co/lib/container"
	"go.atoms.co/lib/mapx"
	"go.atoms.co/slicex"
	"fmt"
	"time"
)

// WorkerID is a globally unique id for workers.
type WorkerID = location.InstanceID

// Worker is a temporary entity that can be assigned work. All active and allocated grants are covered
// by a single renewable lease.
type Worker struct {
	ID       WorkerID
	Location location.Location
}

func NewWorker(inst location.Instance) Worker {
	return Worker{
		ID:       inst.ID(),
		Location: inst.Location(),
	}
}

func (w Worker) String() string {
	return fmt.Sprintf("%v@%v", w.ID, w.Location)
}

type WorkerState string

const (
	Attached  WorkerState = "attached"
	Suspended WorkerState = "suspended" // attached, but excluded for allocation
	Detached  WorkerState = "detached"
)

type WorkerInfo struct {
	Instance Worker
	State    WorkerState
	Lease    time.Time
}

func (w WorkerInfo) String() string {
	return fmt.Sprintf("%v[state=%v, lease=%v]", w.Instance, w.State, w.Lease)
}

// live is a grant with worker lease expiration.
type live[T, D comparable] struct {
	work     Work[T, D]
	grant    GrantID
	state    GrantState
	assigned time.Time
}

// worker holds worker lease and grants. If a grant is revoked, the current worker lease sticks as
// the expiration.
type worker[T, D comparable] struct {
	info WorkerInfo

	live    map[T]live[T, D]  // active + allocated
	revoked map[T]Grant[T, D] // revoked w/ expiration: load ignored

	load  AdjustedLoad // adjusted load
	place map[T]Load   // placement penalty if non-zero
	colo  map[T]Load   // colocation penalty if non-zero
}

func newWorker[T, D comparable](inst Worker, state WorkerState, lease time.Time) *worker[T, D] {
	return &worker[T, D]{
		info: WorkerInfo{
			Instance: inst,
			State:    state,
			Lease:    lease,
		},
		live:    map[T]live[T, D]{},
		revoked: map[T]Grant[T, D]{},
		place:   map[T]Load{},
		colo:    map[T]Load{},
	}
}

func (w *worker[T, D]) Assignments() Assignments[T, D] {
	live := mapx.MapValues(w.live, w.ToGrant)
	return Assignments[T, D]{
		Active:    slicex.Filter(live, Grant[T, D].IsActive),
		Allocated: slicex.Filter(live, Grant[T, D].IsAllocated),
		Revoked:   mapx.Values(w.revoked),
	}
}

func (w *worker[T, D]) Live() map[T]Work[T, D] {
	return mapx.Map(w.live, func(k T, v live[T, D]) (T, Work[T, D]) {
		return k, v.work
	})
}

func (w *worker[T, D]) LiveWith(list ...Work[T, D]) map[T]Work[T, D] {
	ret := w.Live()
	for _, work := range list {
		ret[work.Unit] = work
	}
	return ret
}

func (w *worker[T, D]) LiveWithout(list ...T) map[T]Work[T, D] {
	ret := w.Live()
	for _, t := range list {
		delete(ret, t)
	}
	return ret
}

func (w *worker[T, D]) ToGrant(l live[T, D]) Grant[T, D] {
	return NewGrant(l.grant, l.state, l.work.Unit, l.work.Domain, w.info.Instance.ID, l.assigned, w.info.Lease)
}

func (w *worker[T, D]) Less(o *worker[T, D]) bool {
	if a, b := w.load.Total(), o.load.Total(); a != b {
		return a < b
	}
	return len(w.live) < len(o.live)
}

func (w *worker[T, D]) String() string {
	return fmt.Sprintf("%v[load=%v]", w.info, w.load)
}

func newWorkerHeap[T, D comparable]() *container.Heap[*worker[T, D]] {
	return container.NewHeap[*worker[T, D]](func(a, b *worker[T, D]) bool { return a.Less(b) })
}
