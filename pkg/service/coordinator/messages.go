package coordinator

import (
	"go.atoms.co/splitter/lib/service/session"
	"go.atoms.co/lib/encoding/protox"
	"go.atoms.co/splitter/pkg/model"
	splitterprivatepb "go.atoms.co/splitter/pb/private"
)

type ConnectMessage struct {
	pb *splitterprivatepb.ConnectMessage
}

func WrapConnectMessage(pb *splitterprivatepb.ConnectMessage) ConnectMessage {
	return ConnectMessage{pb: pb}
}

func UnwrapConnectMessage(m ConnectMessage) *splitterprivatepb.ConnectMessage {
	return m.pb
}

func NewConnectConsumerMessage(m model.ConsumerMessage) ConnectMessage {
	return WrapConnectMessage(&splitterprivatepb.ConnectMessage{
		Msg: &splitterprivatepb.ConnectMessage_Consumer{
			Consumer: model.UnwrapConsumerMessage(m),
		},
	})
}

func NewConnectSessionMessage(m session.Message) ConnectMessage {
	return WrapConnectMessage(&splitterprivatepb.ConnectMessage{
		Msg: &splitterprivatepb.ConnectMessage_Session{
			Session: session.UnwrapMessage(m),
		},
	})
}

// HandleRequest is an internal coordinator handle request jacket for routing. Not thread-safe.
type HandleRequest struct {
	Proto *splitterprivatepb.CoordinatorHandleRequest
}

func NewHandleCoordinatorOperationRequest(service model.QualifiedServiceName, req *splitterprivatepb.CoordinatorOperationRequest) HandleRequest {
	return HandleRequest{
		Proto: &splitterprivatepb.CoordinatorHandleRequest{
			Service: service.ToProto(),
			Req: &splitterprivatepb.CoordinatorHandleRequest_Operation{
				Operation: req,
			},
		},
	}
}

func NewHandleCoordinatorOperationResponse(req *splitterprivatepb.CoordinatorOperationResponse) *splitterprivatepb.CoordinatorHandleResponse {
	return &splitterprivatepb.CoordinatorHandleResponse{
		Resp: &splitterprivatepb.CoordinatorHandleResponse_Operation{
			Operation: req,
		},
	}
}

func (m HandleRequest) Service() model.QualifiedServiceName {
	name, _ := model.ParseQualifiedServiceName(m.Proto.GetService())
	return name
}

func (m HandleRequest) MessageType() string {
	switch {
	case m.Proto.GetOperation() != nil:
		return "operation"
	default:
		return "unknown"
	}
}

func (m HandleRequest) String() string {
	return protox.CompactTextString(m.Proto)
}
