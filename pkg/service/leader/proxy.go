package leader

import (
	"context"
	"go.atoms.co/splitter/lib/service/session"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pb/private"
	"github.com/golang/protobuf/proto"
)

type Resolver = model.Resolver[internal_v1.LeaderServiceClient, model.DomainKey]

// Proxy is a local proxy for accessing the leader, if present. Returns ErrNotOwned if not.
type Proxy interface {
	Join(ctx context.Context, sid session.ID, in <-chan Message) (<-chan Message, error)
	Handle(ctx context.Context, request HandleRequest) (*internal_v1.LeaderHandleResponse, error)
}

// HandleRequest is an internal leader handle request jacket for routing. Not threadsafe.
type HandleRequest struct {
	Proto *internal_v1.LeaderHandleRequest
}

func NewHandleTenantRequest(req *internal_v1.TenantRequest) HandleRequest {
	return HandleRequest{
		Proto: &internal_v1.LeaderHandleRequest{
			Req: &internal_v1.LeaderHandleRequest_Tenant{
				Tenant: req,
			},
		},
	}
}

func NewHandleServiceRequest(req *internal_v1.ServiceRequest) HandleRequest {
	return HandleRequest{
		Proto: &internal_v1.LeaderHandleRequest{
			Req: &internal_v1.LeaderHandleRequest_Service{
				Service: req,
			},
		},
	}
}

func NewHandleDomainRequest(req *internal_v1.DomainRequest) HandleRequest {
	return HandleRequest{
		Proto: &internal_v1.LeaderHandleRequest{
			Req: &internal_v1.LeaderHandleRequest_Domain{
				Domain: req,
			},
		},
	}
}

func NewHandlePlacementRequest(req *internal_v1.PlacementRequest) HandleRequest {
	return HandleRequest{
		Proto: &internal_v1.LeaderHandleRequest{
			Req: &internal_v1.LeaderHandleRequest_Placement{
				Placement: req,
			},
		},
	}
}

func NewHandleOperationRequest(req *internal_v1.OperationRequest) HandleRequest {
	return HandleRequest{
		Proto: &internal_v1.LeaderHandleRequest{
			Req: &internal_v1.LeaderHandleRequest_Operation{
				Operation: req,
			},
		},
	}
}

func (m HandleRequest) IsMutation() bool {
	switch {
	case m.Proto.GetTenant() != nil:
		pb := m.Proto.GetTenant()
		return pb.GetNew() != nil || pb.GetUpdate() != nil || pb.GetDelete() != nil

	case m.Proto.GetService() != nil:
		pb := m.Proto.GetService()
		return pb.GetNew() != nil || pb.GetUpdate() != nil || pb.GetDelete() != nil

	case m.Proto.GetDomain() != nil:
		pb := m.Proto.GetDomain()
		return pb.GetNew() != nil || pb.GetUpdate() != nil || pb.GetDelete() != nil

	case m.Proto.GetPlacement() != nil:
		pb := m.Proto.GetPlacement()
		return pb.GetNew() != nil || pb.GetUpdate() != nil || pb.GetDelete() != nil

	case m.Proto.GetOperation() != nil:
		pb := m.Proto.GetOperation()
		return pb.GetRestore() != nil

	default:
		return false
	}

}

func (m HandleRequest) String() string {
	return proto.CompactTextString(m.Proto)
}

func NewHandleTenantResponse(req *internal_v1.TenantResponse) *internal_v1.LeaderHandleResponse {
	return &internal_v1.LeaderHandleResponse{
		Resp: &internal_v1.LeaderHandleResponse_Tenant{
			Tenant: req,
		},
	}
}

func NewHandleServiceResponse(req *internal_v1.ServiceResponse) *internal_v1.LeaderHandleResponse {
	return &internal_v1.LeaderHandleResponse{
		Resp: &internal_v1.LeaderHandleResponse_Service{
			Service: req,
		},
	}
}

func NewHandleDomainResponse(req *internal_v1.DomainResponse) *internal_v1.LeaderHandleResponse {
	return &internal_v1.LeaderHandleResponse{
		Resp: &internal_v1.LeaderHandleResponse_Domain{
			Domain: req,
		},
	}
}

func NewHandlePlacementResponse(req *internal_v1.PlacementResponse) *internal_v1.LeaderHandleResponse {
	return &internal_v1.LeaderHandleResponse{
		Resp: &internal_v1.LeaderHandleResponse_Placement{
			Placement: req,
		},
	}
}

func NewHandleOperationResponse(req *internal_v1.OperationResponse) *internal_v1.LeaderHandleResponse {
	return &internal_v1.LeaderHandleResponse{
		Resp: &internal_v1.LeaderHandleResponse_Operation{
			Operation: req,
		},
	}
}
