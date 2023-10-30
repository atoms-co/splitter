package allocation

import (
	"go.atoms.co/splitter/lib/service/location"
	"fmt"
)

// Load is a non-negative scoring unit for work load and assignment penalties. A zero penalty indicates an
// optimal but not necessarily unique assignment for the underlying work.
type Load int64

// Work is a unit of assignable work with an intrinsic load or cost. Work is grouped into domains.
// Within each domain, work is aimed to be evenly distributed.
type Work[T, D comparable] struct {
	Unit     T
	Domain   D
	Location location.Location
	Load     Load // intrinsic load
}

func (w Work[T, D]) String() string {
	return fmt.Sprintf("%v@%v[domain=%v, load=%v]", w.Unit, w.Location, w.Domain, w.Load)
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
