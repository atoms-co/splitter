package core

import (
	"github.com/golang/protobuf/proto"

	"go.atoms.co/splitter/lib/service/session"
	"go.atoms.co/splitter/pkg/model"
	splitterprivatepb "go.atoms.co/splitter/pb/private"
)

type ObserverClientMessage struct {
	pb *splitterprivatepb.ObserverClientMessage
}

// NewObserverClientMessage creates a new observer client message with a session message.
func NewObserverClientMessage(m session.Message) ObserverClientMessage {
	return WrapObserverClientMessage(&splitterprivatepb.ObserverClientMessage{
		Msg: &splitterprivatepb.ObserverClientMessage_Session{
			Session: session.UnwrapMessage(m),
		},
	})
}

// NewObserverRegisterMessage is a factory method that creates new observer registration messages.
func NewObserverRegisterMessage(observer model.Instance, service model.QualifiedServiceName) ObserverClientMessage {
	register := &splitterprivatepb.ObserverClientMessage_Register{
		Observer: model.UnwrapInstance(observer),
		Service:  service.ToProto(),
	}

	return WrapObserverClientMessage(&splitterprivatepb.ObserverClientMessage{
		Msg: &splitterprivatepb.ObserverClientMessage_Register_{
			Register: register,
		},
	})
}

func WrapObserverClientMessage(pb *splitterprivatepb.ObserverClientMessage) ObserverClientMessage {
	return ObserverClientMessage{pb: pb}
}

func UnwrapObserverClientMessage(m ObserverClientMessage) *splitterprivatepb.ObserverClientMessage {
	return m.pb
}

func (m ObserverClientMessage) IsRegister() bool {
	return m.pb.GetRegister() != nil
}

func (m ObserverClientMessage) Register() (ObserverRegisterMessage, bool) {
	if r := m.pb.GetRegister(); r != nil {
		return WrapObserverRegisterMessage(r), true
	}
	return ObserverRegisterMessage{}, false
}

func (m ObserverClientMessage) Session() (session.Message, bool) {
	if s := m.pb.GetSession(); s != nil {
		return session.WrapMessage(s), true
	}
	return session.Message{}, false
}

func (m ObserverClientMessage) String() string {
	return proto.MarshalTextString(m.pb)
}

type ObserverRegisterMessage struct {
	pb *splitterprivatepb.ObserverClientMessage_Register
}

func WrapObserverRegisterMessage(pb *splitterprivatepb.ObserverClientMessage_Register) ObserverRegisterMessage {
	return ObserverRegisterMessage{pb: pb}
}

func UnwrapObserverRegisterMessage(m ObserverRegisterMessage) *splitterprivatepb.ObserverClientMessage_Register {
	return m.pb
}

func (m ObserverRegisterMessage) Observer() model.Instance {
	return model.WrapInstance(m.pb.GetObserver())
}

func (m ObserverRegisterMessage) Service() (model.QualifiedServiceName, error) {
	return model.ParseQualifiedServiceName(m.pb.GetService())
}

func (m ObserverRegisterMessage) String() string {
	return proto.MarshalTextString(m.pb)
}

type ObserverServerMessage struct {
	pb *splitterprivatepb.ObserverServerMessage
}

func WrapObserverServerMessage(pb *splitterprivatepb.ObserverServerMessage) ObserverServerMessage {
	return ObserverServerMessage{pb: pb}
}

func UnwrapObserverServerMessage(m ObserverServerMessage) *splitterprivatepb.ObserverServerMessage {
	return m.pb
}

func NewObserverClusterMessage(msg model.ClusterMessage) ObserverServerMessage {
	return ObserverServerMessage{
		pb: &splitterprivatepb.ObserverServerMessage{
			Msg: &splitterprivatepb.ObserverServerMessage_Cluster{
				Cluster: model.UnwrapClusterMessage(msg),
			},
		},
	}
}

func NewObserverServerMessage(m session.Message) ObserverServerMessage {
	return ObserverServerMessage{pb: &splitterprivatepb.ObserverServerMessage{
		Msg: &splitterprivatepb.ObserverServerMessage_Session{
			Session: session.UnwrapMessage(m),
		},
	}}
}

func (m ObserverServerMessage) IsCluster() bool {
	return m.pb.GetCluster() != nil
}

func (m ObserverServerMessage) ClusterMessage() (model.ClusterMessage, bool) {
	if c := m.pb.GetCluster(); c != nil {
		return model.WrapClusterMessage(c), true
	}
	return model.ClusterMessage{}, false
}

func (m ObserverServerMessage) String() string {
	return proto.MarshalTextString(m.pb)
}
