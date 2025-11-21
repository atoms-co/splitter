package location

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"

	locationpb "go.atoms.co/splitter/lib/service/location/pb"
)

// InstanceID identifies a component instance. It is transient and bound in-memory
type InstanceID string

// Instance represents a component instance in a pure, serializable form. Mainly for debugging. Immutable.
type Instance struct {
	pb *locationpb.Instance
}

// InstanceOption represents an option for a new Instance
type InstanceOption func(Instance)

// WithName sets a name for an instance. For debugging purposes.
func WithName(name string) InstanceOption {
	return func(instance Instance) {
		instance.pb.Name = name
	}
}

func WithInstanceID(id InstanceID) InstanceOption {
	return func(instance Instance) {
		instance.pb.Id = string(id)
	}
}

func WithInstanceCreatedAt(ts time.Time) InstanceOption {
	return func(instance Instance) {
		instance.pb.Created = timestamppb.New(ts)
	}
}

func NewInstance(loc Location, opts ...InstanceOption) Instance {
	ret := Instance{pb: &locationpb.Instance{
		Id:       uuid.NewString(),
		Location: loc.ToProto(),
		Created:  timestamppb.New(time.Now()),
	}}
	for _, opt := range opts {
		opt(ret)
	}
	return ret
}

func NewNamedInstance(name string, loc Location, opts ...InstanceOption) Instance {
	return NewInstance(loc, append(opts, WithName(name))...)
}

func WrapInstance(pb *locationpb.Instance) Instance {
	return Instance{pb: pb}
}

func UnwrapInstance(m Instance) *locationpb.Instance {
	return m.pb
}

func (c Instance) ID() InstanceID {
	return InstanceID(c.pb.GetId())
}

func (c Instance) Location() Location {
	return Parse(c.pb.GetLocation())
}

func (c Instance) Name() string {
	return c.pb.GetName()
}

func (c Instance) Created() time.Time {
	return c.pb.GetCreated().AsTime()
}

func (c Instance) String() string {
	if c.pb == nil {
		return "?"
	}
	if c.Name() == "" {
		return fmt.Sprintf("%v[%v]@%v", c.ID(), c.Location(), c.Created().Unix())
	}
	return fmt.Sprintf("%v/%v[%v]@%v", c.Name(), c.ID(), c.Location(), c.Created().Unix())
}
