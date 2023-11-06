package allocation

import (
	"go.atoms.co/splitter/lib/service/location"
	"fmt"
)

// Load is a non-negative scoring unit for work load and assignment penalties. A zero penalty indicates an
// optimal but not necessarily unique assignment for the underlying work.
type Load int64

// Work is a unit of assignable work with an intrinsic load or cost.
type Work[T comparable] struct {
	Unit     T
	Location location.Location
	Load     Load // intrinsic load
}

func (w Work[T]) String() string {
	return fmt.Sprintf("%v@%v[load=%v]", w.Unit, w.Location, w.Load)
}

// AdjustedLoad holds intrinsic load w/ placement and colocation penalties.
type AdjustedLoad struct {
	Load, Place, Colo Load
}

func (l AdjustedLoad) Total() Load {
	return l.Load + l.Place + l.Colo
}

func (l AdjustedLoad) String() string {
	return fmt.Sprintf("%v[load=%v, place=%v, colo=%v]", l.Total(), l.Load, l.Place, l.Colo)
}
