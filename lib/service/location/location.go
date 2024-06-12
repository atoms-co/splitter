package location

import (
	"fmt"

	"go.atoms.co/splitter/lib/service/location/pb"
)

// Region represents a persistence-layer region for affinity.
type Region string

// Node represents a virtual or physical machine or pod.
type Node string

// Location is a location for component observability. System components move around. Can be used for debugging.
type Location struct {
	Region Region
	Node   Node
}

func New(region Region, node Node) Location {
	return Location{
		Region: region,
		Node:   node,
	}
}

func Parse(pb *location_v1.Location) Location {
	return Location{
		Region: Region(pb.GetRegion()),
		Node:   Node(pb.GetNode()),
	}
}

func (l Location) ToProto() *location_v1.Location {
	return &location_v1.Location{
		Region: string(l.Region),
		Node:   string(l.Node),
	}
}

func (l Location) String() string {
	return fmt.Sprintf("%v/%v", l.Region, l.Node)
}
