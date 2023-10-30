package allocation

import (
	"go.atoms.co/splitter/lib/service/location"
	"go.atoms.co/lib/mapx"
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
	Lease    time.Time
}

func (w Worker) String() string {
	return fmt.Sprintf("%v@%v", w.ID, w.Location)
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
	info Worker

	live    map[T]live[T, D]  // active + allocated
	revoked map[T]Grant[T, D] // revoked w/ expiration: load ignored

	load  AdjustedLoad // adjusted load
	place map[T]Load   // placement penalty if non-zero
	colo  map[T]Load   // colocation penalty if non-zero
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
	return NewGrant(l.grant, l.state, l.work.Unit, l.work.Domain, w.info.ID, l.assigned, w.info.Lease)
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
