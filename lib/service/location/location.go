package location

import (
	"fmt"
	"os"

	locationpb "go.atoms.co/splitter/lib/service/location/pb"
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

// NewFromEnv creates a Location from APP_REGION and HOSTNAME environment variables, if present. May be overridden
// by flags. Defaults to "global" region with "localhost".
func NewFromEnv() Location {
	region := readEnv("APP_REGION", "global")
	node := readEnv("HOSTNAME", "localhost")
	return New(Region(region), Node(node))
}

func Parse(pb *locationpb.Location) Location {
	return Location{
		Region: Region(pb.GetRegion()),
		Node:   Node(pb.GetNode()),
	}
}

func (l Location) ToProto() *locationpb.Location {
	return &locationpb.Location{
		Region: string(l.Region),
		Node:   string(l.Node),
	}
}

func (l Location) String() string {
	return fmt.Sprintf("%v/%v", l.Region, l.Node)
}

func readEnv(name, def string) string {
	if v := os.Getenv(name); len(v) > 0 {
		return v
	}
	return def
}
