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
type live[T comparable] struct {
	work     Work[T]
	grant    GrantID
	state    GrantState
	assigned time.Time
}

// worker holds worker lease and grants. If a grant is revoked, the current worker lease sticks as
// the expiration.
type worker[T comparable] struct {
	info WorkerInfo

	live    map[T]live[T]  // active + allocated
	revoked map[T]Grant[T] // revoked w/ expiration: load ignored

	load  AdjustedLoad // adjusted load
	place map[T]Load   // placement penalty if non-zero
	colo  map[T]Load   // colocation penalty if non-zero
}

func newWorker[T comparable](inst Worker, state WorkerState, lease time.Time) *worker[T] {
	return &worker[T]{
		info: WorkerInfo{
			Instance: inst,
			State:    state,
			Lease:    lease,
		},
		live:    map[T]live[T]{},
		revoked: map[T]Grant[T]{},
		place:   map[T]Load{},
		colo:    map[T]Load{},
	}
}

func (w *worker[T]) Assignments() Assignments[T] {
	live := mapx.MapValues(w.live, w.ToGrant)
	return Assignments[T]{
		Active:    slicex.Filter(live, Grant[T].IsActive),
		Allocated: slicex.Filter(live, Grant[T].IsAllocated),
		Revoked:   mapx.Values(w.revoked),
	}
}

func (w *worker[T]) Live() map[T]Work[T] {
	return mapx.Map(w.live, func(k T, v live[T]) (T, Work[T]) {
		return k, v.work
	})
}

func (w *worker[T]) LiveWith(list ...Work[T]) map[T]Work[T] {
	ret := w.Live()
	for _, work := range list {
		ret[work.Unit] = work
	}
	return ret
}

func (w *worker[T]) LiveWithout(list ...T) map[T]Work[T] {
	ret := w.Live()
	for _, t := range list {
		delete(ret, t)
	}
	return ret
}

func (w *worker[T]) Grant(t T) (Grant[T], bool) {
	if l, ok := w.live[t]; ok {
		return w.ToGrant(l), true
	}
	g, ok := w.revoked[t]
	return g, ok
}

func (w *worker[T]) ToGrant(l live[T]) Grant[T] {
	return NewGrant(l.grant, l.state, l.work.Unit, w.info.Instance.ID, l.assigned, w.info.Lease)
}

func (w *worker[T]) Less(o *worker[T]) bool {
	if a, b := w.load.Total(), o.load.Total(); a != b {
		return a < b
	}
	return len(w.live) < len(o.live)
}

func (w *worker[T]) String() string {
	return fmt.Sprintf("%v[load=%v]", w.info, w.load)
}

func newWorkerHeap[T comparable]() *container.Heap[*worker[T]] {
	return container.NewHeap[*worker[T]](func(a, b *worker[T]) bool { return a.Less(b) })
}
