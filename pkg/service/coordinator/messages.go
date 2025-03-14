package coordinator

import (
	"github.com/golang/protobuf/proto"

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

// HandleRequest is an internal coordinator handle request jacket for routing. Not thread-safe.
type HandleRequest struct {
	Proto *internal_v1.CoordinatorHandleRequest
}

func NewHandleCoordinatorOperationRequest(service model.QualifiedServiceName, req *internal_v1.CoordinatorOperationRequest) HandleRequest {
	return HandleRequest{
		Proto: &internal_v1.CoordinatorHandleRequest{
			Service: service.ToProto(),
			Req: &internal_v1.CoordinatorHandleRequest_Operation{
				Operation: req,
			},
		},
	}
}

func NewHandleCoordinatorOperationResponse(req *internal_v1.CoordinatorOperationResponse) *internal_v1.CoordinatorHandleResponse {
	return &internal_v1.CoordinatorHandleResponse{
		Resp: &internal_v1.CoordinatorHandleResponse_Operation{
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
	return proto.CompactTextString(m.Proto)
}
