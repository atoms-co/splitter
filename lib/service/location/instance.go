package location

import (
	"go.atoms.co/splitter/lib/service/location/pb"
	"fmt"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"
	"time"
)

// InstanceID identifies a component instance. It is transient and bound in-memory
type InstanceID string

// Instance represents a component instance in a pure, serializable form. Mainly for debugging. Immutable.
type Instance struct {
	pb *location_v1.Instance
}

func NewInstance(loc Location) Instance {
	return Instance{pb: &location_v1.Instance{
		Id:       uuid.NewString(),
		Location: loc.ToProto(),
		Created:  timestamppb.New(time.Now()),
	}}
}

func WrapInstance(pb *location_v1.Instance) Instance {
	return Instance{pb: pb}
}

func UnwrapInstance(m Instance) *location_v1.Instance {
	return m.pb
}

func (c Instance) ID() InstanceID {
	return InstanceID(c.pb.GetId())
}

func (c Instance) Location() Location {
	return Parse(c.pb.GetLocation())
}

func (c Instance) Created() time.Time {
	return c.pb.GetCreated().AsTime()
}

func (c Instance) String() string {
	return fmt.Sprintf("%v[%v]@%v", c.ID(), c.Location(), c.Created().Unix())
}
