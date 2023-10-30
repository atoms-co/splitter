package model

import (
	"go.atoms.co/splitter/lib/service/location"
	"go.atoms.co/splitter/pb"
	"fmt"
)

type InstanceID = location.InstanceID

type Instance struct {
	pb *public_v1.Instance
}

func NewInstance(instance location.Instance, endpoint string) Instance {
	return WrapInstance(&public_v1.Instance{
		Client:   location.UnwrapInstance(instance),
		Endpoint: endpoint,
	})
}

func WrapInstance(pb *public_v1.Instance) Instance {
	return Instance{pb: pb}
}

func UnwrapInstance(instance Instance) *public_v1.Instance {
	return instance.pb
}

func (i Instance) ID() InstanceID {
	return InstanceID(i.pb.GetClient().GetId())
}

func (i Instance) Client() location.Instance {
	return location.WrapInstance(i.pb.GetClient())
}

func (i Instance) Location() location.Location {
	return location.Parse(i.pb.GetClient().GetLocation())
}

func (i Instance) Endpoint() string {
	return i.pb.GetEndpoint()
}

func (i Instance) String() string {
	return fmt.Sprintf("%v=%v", i.Location(), i.Endpoint())
}
