package model

import (
	"fmt"

	"go.atoms.co/splitter/lib/service/location"
	splitterpb "go.atoms.co/splitter/pb"
)

type InstanceID = location.InstanceID

type Instance struct {
	pb *splitterpb.Instance
}

func NewInstance(instance location.Instance, endpoint string) Instance {
	return WrapInstance(&splitterpb.Instance{
		Instance: location.UnwrapInstance(instance),
		Endpoint: endpoint,
	})
}

func WrapInstance(pb *splitterpb.Instance) Instance {
	return Instance{pb: pb}
}

func UnwrapInstance(instance Instance) *splitterpb.Instance {
	return instance.pb
}

func (i Instance) ID() InstanceID {
	return InstanceID(i.pb.GetInstance().GetId())
}

func (i Instance) Instance() location.Instance {
	return location.WrapInstance(i.pb.GetInstance())
}

func (i Instance) Location() location.Location {
	return location.Parse(i.pb.GetInstance().GetLocation())
}

func (i Instance) Endpoint() string {
	return i.pb.GetEndpoint()
}

func (i Instance) String() string {
	return fmt.Sprintf("%v[location=%v,endpoint=%v]", i.ID(), i.Location(), i.Endpoint())
}
