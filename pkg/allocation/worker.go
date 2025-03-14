package allocation

import (
	"fmt"
	"time"

	"go.atoms.co/lib/container"
	"go.atoms.co/lib/mapx"
	"go.atoms.co/slicex"
)

// Worker is a temporary entity that can be assigned work. All active and allocated grants are covered
// by a single renewable lease.
type Worker[K comparable, V any] struct {
	ID   K
	Data V
}

func NewWorker[K comparable, V any](id K, data V) Worker[K, V] {
	return Worker[K, V]{
		ID:   id,
		Data: data,
	}
}

func (w Worker[K, V]) String() string {
	return fmt.Sprintf("Worker[id=%v, data=%v]", w.ID, w.Data)
}

type WorkerState string

const (
	Attached  WorkerState = "attached"
	Suspended WorkerState = "suspended" // attached, but excluded for allocation
	Detached  WorkerState = "detached"
)

type WorkerInfo[K comparable, V any] struct {
	Instance Worker[K, V]
	State    WorkerState
	Limit    Load // Capacity limit for intrinsic Load
	Lease    time.Time
}

func (w WorkerInfo[K, V]) ID() K {
	return w.Instance.ID
}

func (w WorkerInfo[K, V]) String() string {
	return fmt.Sprintf("WorkerInfo[instance=%v, state=%v, lease=%v]", w.Instance, w.State, w.Lease)
}

// live is a grant with worker lease expiration.
type live[T comparable, W any] struct {
	work     Work[T, W]
	grant    GrantID
	state    GrantState
	mod      GrantModifier
	assigned time.Time
}

// worker holds worker lease and grants. If a grant is revoked, the current worker lease sticks as
// the expiration.
type worker[T comparable, W any, K comparable, V any] struct {
	info WorkerInfo[K, V]

	live    map[T]live[T, W]  // active + allocated
	revoked map[T]Grant[T, K] // revoked w/ expiration: load ignored

	load  AdjustedLoad // adjusted load
	place map[T]Load   // placement penalty if non-zero
	colo  map[T]Load   // colocation penalty if non-zero
}

func newWorker[T comparable, W any, K comparable, V any](inst Worker[K, V], state WorkerState, limit Load, lease time.Time) *worker[T, W, K, V] {
	return &worker[T, W, K, V]{
		info: WorkerInfo[K, V]{
			Instance: inst,
			State:    state,
			Limit:    limit,
			Lease:    lease,
		},
		live:    map[T]live[T, W]{},
		revoked: map[T]Grant[T, K]{},
		place:   map[T]Load{},
		colo:    map[T]Load{},
	}
}

func (w *worker[T, W, K, V]) Assignments() Assignments[T, K] {
	live := mapx.MapValues(w.live, w.ToGrant)
	return Assignments[T, K]{
		Active:    slicex.Filter(live, Grant[T, K].IsActive),
		Allocated: slicex.Filter(live, Grant[T, K].IsAllocated),
		Revoked:   mapx.Values(w.revoked),
	}
}

func (w *worker[T, W, K, V]) Live() map[T]Work[T, W] {
	return mapx.Map(w.live, func(k T, v live[T, W]) (T, Work[T, W]) {
		return k, v.work
	})
}

func (w *worker[T, W, K, V]) LiveWith(list ...Work[T, W]) map[T]Work[T, W] {
	ret := w.Live()
	for _, work := range list {
		ret[work.Unit] = work
	}
	return ret
}

func (w *worker[T, W, K, V]) LiveWithout(list ...T) map[T]Work[T, W] {
	ret := w.Live()
	for _, t := range list {
		delete(ret, t)
	}
	return ret
}

func (w *worker[T, W, K, V]) HasCapacity(add Load) bool {
	return w.info.Limit == 0 || (w.load.Load+add) <= w.info.Limit
}

func (w *worker[T, W, K, V]) FullCapacity() bool {
	if w.info.Limit == 0 {
		return false
	}
	return w.load.Load >= w.info.Limit
}

func (w *worker[T, W, K, V]) Grant(t T) (Grant[T, K], bool) {
	if l, ok := w.live[t]; ok {
		return w.ToGrant(l), true
	}
	g, ok := w.revoked[t]
	return g, ok
}

func (w *worker[T, W, K, V]) ToGrant(l live[T, W]) Grant[T, K] {
	return NewGrant(l.grant, l.state, l.mod, l.work.Unit, w.info.Instance.ID, l.assigned, w.info.Lease)
}

func (w *worker[T, W, K, V]) Less(o *worker[T, W, K, V]) bool {
	if a, b := w.load.Total(), o.load.Total(); a != b {
		return a < b
	}
	return len(w.live) < len(o.live)
}

func (w *worker[T, W, K, V]) String() string {
	return fmt.Sprintf("%v[load=%v]", w.info, w.load)
}

func newWorkerHeap[T comparable, W any, K comparable, V any]() *container.Heap[*worker[T, W, K, V]] {
	return container.NewHeap[*worker[T, W, K, V]](func(a, b *worker[T, W, K, V]) bool { return a.Less(b) })
}
