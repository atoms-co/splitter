package coordinator

import (
	"go.atoms.co/splitter/lib/service/session"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pb/private"
)

type ConnectMessage struct {
	pb *internal_v1.ConnectMessage
}

func WrapConnectMessage(pb *internal_v1.ConnectMessage) ConnectMessage {
	return ConnectMessage{pb: pb}
}

func UnwrapConnectMessage(m ConnectMessage) *internal_v1.ConnectMessage {
	return m.pb
}

func NewConnectConsumerMessage(m model.ConsumerMessage) ConnectMessage {
	return WrapConnectMessage(&internal_v1.ConnectMessage{
		Msg: &internal_v1.ConnectMessage_Consumer{
			Consumer: model.UnwrapConsumerMessage(m),
		},
	})
}

func NewConnectSessionMessage(m session.Message) ConnectMessage {
	return WrapConnectMessage(&internal_v1.ConnectMessage{
		Msg: &internal_v1.ConnectMessage_Session{
			Session: session.UnwrapMessage(m),
		},
	})
}
