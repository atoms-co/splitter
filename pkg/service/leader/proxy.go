package leader

import (
	"context"

	"go.atoms.co/splitter/lib/service/session"
	"go.atoms.co/lib/encoding/protox"
	"go.atoms.co/splitter/pkg/model"
	splitterprivatepb "go.atoms.co/splitter/pb/private"
)

type Resolver = model.SimpleResolver[splitterprivatepb.LeaderServiceClient, model.DomainKey]

// Proxy is a local proxy for accessing the leader, if present. Returns ErrNotOwned if not.
type Proxy interface {
	// Join handles connection of a worker to the leader
	// Returns a channel with messages for the worker or a logical error.
	Join(ctx context.Context, sid session.ID, in <-chan Message) (<-chan Message, error)

	Handle(ctx context.Context, request HandleRequest) (*splitterprivatepb.LeaderHandleResponse, error)
}

// HandleRequest is an internal leader handle request jacket for routing. Not threadsafe.
type HandleRequest struct {
	Proto *splitterprivatepb.LeaderHandleRequest
}

func NewHandleTenantRequest(req *splitterprivatepb.TenantRequest) HandleRequest {
	return HandleRequest{
		Proto: &splitterprivatepb.LeaderHandleRequest{
			Req: &splitterprivatepb.LeaderHandleRequest_Tenant{
				Tenant: req,
			},
		},
	}
}

func NewHandleServiceRequest(req *splitterprivatepb.ServiceRequest) HandleRequest {
	return HandleRequest{
		Proto: &splitterprivatepb.LeaderHandleRequest{
			Req: &splitterprivatepb.LeaderHandleRequest_Service{
				Service: req,
			},
		},
	}
}

func NewHandleDomainRequest(req *splitterprivatepb.DomainRequest) HandleRequest {
	return HandleRequest{
		Proto: &splitterprivatepb.LeaderHandleRequest{
			Req: &splitterprivatepb.LeaderHandleRequest_Domain{
				Domain: req,
			},
		},
	}
}

func NewHandlePlacementRequest(req *splitterprivatepb.PlacementRequest) HandleRequest {
	return HandleRequest{
		Proto: &splitterprivatepb.LeaderHandleRequest{
			Req: &splitterprivatepb.LeaderHandleRequest_Placement{
				Placement: req,
			},
		},
	}
}

func NewHandleOperationRequest(req *splitterprivatepb.OperationRequest) HandleRequest {
	return HandleRequest{
		Proto: &splitterprivatepb.LeaderHandleRequest{
			Req: &splitterprivatepb.LeaderHandleRequest_Operation{
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

func (m HandleRequest) MessageType() string {
	switch {
	case m.Proto.GetTenant() != nil:
		return "tenant"
	case m.Proto.GetService() != nil:
		return "service"
	case m.Proto.GetDomain() != nil:
		return "domain"
	case m.Proto.GetPlacement() != nil:
		return "placement"
	case m.Proto.GetOperation() != nil:
		return "operation"
	default:
		return "unknown"
	}
}

func (m HandleRequest) String() string {
	return protox.CompactTextString(m.Proto)
}

func NewHandleTenantResponse(req *splitterprivatepb.TenantResponse) *splitterprivatepb.LeaderHandleResponse {
	return &splitterprivatepb.LeaderHandleResponse{
		Resp: &splitterprivatepb.LeaderHandleResponse_Tenant{
			Tenant: req,
		},
	}
}

func NewHandleServiceResponse(req *splitterprivatepb.ServiceResponse) *splitterprivatepb.LeaderHandleResponse {
	return &splitterprivatepb.LeaderHandleResponse{
		Resp: &splitterprivatepb.LeaderHandleResponse_Service{
			Service: req,
		},
	}
}

func NewHandleDomainResponse(req *splitterprivatepb.DomainResponse) *splitterprivatepb.LeaderHandleResponse {
	return &splitterprivatepb.LeaderHandleResponse{
		Resp: &splitterprivatepb.LeaderHandleResponse_Domain{
			Domain: req,
		},
	}
}

func NewHandlePlacementResponse(req *splitterprivatepb.PlacementResponse) *splitterprivatepb.LeaderHandleResponse {
	return &splitterprivatepb.LeaderHandleResponse{
		Resp: &splitterprivatepb.LeaderHandleResponse_Placement{
			Placement: req,
		},
	}
}

func NewHandleOperationResponse(req *splitterprivatepb.OperationResponse) *splitterprivatepb.LeaderHandleResponse {
	return &splitterprivatepb.LeaderHandleResponse{
		Resp: &splitterprivatepb.LeaderHandleResponse_Operation{
			Operation: req,
		},
	}
}
