package location

import (
	"atoms.co/lib-go/pkg/dist/region"
	"go.atoms.co/splitter/lib/service/location/pb"
	"fmt"
)

// Location is a location for component observability. System components move around. Can be used for debugging.
type Location struct {
	pb *location_v1.Location
}

func New(region region.Region, node string) Location {
	return Location{pb: &location_v1.Location{
		Region: string(region),
		Node:   node,
	}}
}

func Wrap(pb *location_v1.Location) Location {
	return Location{pb: pb}
}

func Unwrap(l Location) *location_v1.Location {
	return l.pb
}

func (l Location) Region() region.Region {
	return region.Region(l.pb.GetRegion())
}

func (l Location) Node() string {
	return l.pb.GetNode()
}

func (l Location) String() string {
	return fmt.Sprintf("%v/%v", l.Region(), l.Node())
}
