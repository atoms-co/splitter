package model

import (
	"go.atoms.co/splitter/lib/service/location"
	"go.atoms.co/splitter/lib/service/session"
	"go.atoms.co/splitter/pb"
	"fmt"
	"github.com/golang/protobuf/proto"
)

type InstanceID = location.InstanceID

type Instance struct {
	pb *public_v1.Instance
}

func WrapInstance(pb *public_v1.Instance) Instance {
	return Instance{pb: pb}
}

func UnwrapInstance(instance Instance) *public_v1.Instance {
	return instance.pb
}

func (i Instance) Location() location.Instance {
	return location.WrapInstance(i.pb.GetClient())
}

func (i Instance) Endpoint() string {
	return i.pb.GetEndpoint()
}

func (i Instance) String() string {
	return fmt.Sprintf("%v=%v", i.Location(), i.Endpoint())
}

type RegisterMessage struct {
	pb *public_v1.Register
}

func WrapRegisterMessage(pb *public_v1.Register) RegisterMessage {
	return RegisterMessage{pb: pb}
}

func UnwrapRegisterMessage(m RegisterMessage) *public_v1.Register {
	return m.pb
}

func (m RegisterMessage) TenantName() TenantName {
	return TenantName(m.pb.Tenant)
}

func (m RegisterMessage) Instance() location.Instance {
	return location.WrapInstance(m.pb.GetInstance())
}

func (m RegisterMessage) String() string {
	return proto.MarshalTextString(m.pb)
}

type ConsumerMessage struct {
	pb *public_v1.ConsumerMessage
}

func WrapConsumerMessage(pb *public_v1.ConsumerMessage) ConsumerMessage {
	return ConsumerMessage{pb: pb}
}

func UnwrapConsumerMessage(m ConsumerMessage) *public_v1.ConsumerMessage {
	return m.pb
}

func (m ConsumerMessage) IsRegister() bool {
	return m.pb.GetRegister() != nil
}

func (m ConsumerMessage) Register() (RegisterMessage, bool) {
	if !m.IsRegister() {
		return RegisterMessage{}, false
	}
	return WrapRegisterMessage(m.pb.GetRegister()), true
}

func (m ConsumerMessage) String() string {
	return proto.MarshalTextString(m.pb)
}

type CoordinatorMessage struct {
	pb *public_v1.CoordinatorMessage
}

func WrapCoordinatorMessage(pb *public_v1.CoordinatorMessage) CoordinatorMessage {
	return CoordinatorMessage{pb: pb}
}

func UnwrapCoordinatorMessage(m CoordinatorMessage) *public_v1.CoordinatorMessage {
	return m.pb
}

func NewCoordinatorSessionMessage(m session.Message) CoordinatorMessage {
	return WrapCoordinatorMessage(&public_v1.CoordinatorMessage{
		Msg: &public_v1.CoordinatorMessage_Session{
			Session: session.UnwrapMessage(m),
		},
	})
}

func NewCoordinatorDisconnectMessage() CoordinatorMessage {
	return CoordinatorMessage{
		pb: &public_v1.CoordinatorMessage{
			Msg: &public_v1.CoordinatorMessage_Disconnect_{
				Disconnect: &public_v1.CoordinatorMessage_Disconnect{},
			},
		},
	}
}

func (m CoordinatorMessage) String() string {
	return proto.MarshalTextString(m.pb)
}
