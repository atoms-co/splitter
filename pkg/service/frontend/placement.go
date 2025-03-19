package frontend

import (
	"context"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.atoms.co/lib/log"
	"go.atoms.co/slicex"
	"go.atoms.co/splitter/pkg/core"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pkg/service/leader"
	splitterprivatepb "go.atoms.co/splitter/pb/private"
	splitterpb "go.atoms.co/splitter/pb"
)

const (
	handleTimeout = 20 * time.Second
)

type PlacementService struct {
	internal *InternalPlacementService
}

func NewPlacementService(internal *InternalPlacementService) *PlacementService {
	return &PlacementService{internal: internal}
}

func (p *PlacementService) List(ctx context.Context, request *splitterpb.ListPlacementsRequest) (*splitterpb.ListPlacementsResponse, error) {
	resp, err := p.internal.List(ctx, &splitterprivatepb.ListPlacementsRequest{Tenant: request.GetTenant()})
	if err != nil {
		return nil, err
	}

	return &splitterpb.ListPlacementsResponse{
		Info: slicex.Map(resp.GetInfo(), func(t *splitterprivatepb.InternalPlacementInfo) *splitterpb.PlacementInfo {
			return model.UnwrapPlacementInfo(core.WrapInternalPlacementInfo(t).ToPlacementInfo())
		}),
	}, nil
}

func (p *PlacementService) Info(ctx context.Context, request *splitterpb.InfoPlacementRequest) (*splitterpb.InfoPlacementResponse, error) {
	resp, err := p.internal.Info(ctx, &splitterprivatepb.InfoPlacementRequest{Name: request.GetName()})
	if err != nil {
		return nil, err
	}
	return &splitterpb.InfoPlacementResponse{
		Info: model.UnwrapPlacementInfo(core.WrapInternalPlacementInfo(resp.GetInfo()).ToPlacementInfo()),
	}, nil
}

type InternalPlacementService struct {
	resolver leader.Resolver
	proxy    leader.Proxy
}

func NewInternalPlacementService(proxy leader.Proxy, resolver leader.Resolver) *InternalPlacementService {
	return &InternalPlacementService{proxy: proxy, resolver: resolver}
}

func (i *InternalPlacementService) List(ctx context.Context, request *splitterprivatepb.ListPlacementsRequest) (*splitterprivatepb.ListPlacementsResponse, error) {
	if request.GetTenant() == "" {
		return nil, status.Error(codes.InvalidArgument, "empty tenant")
	}

	resp, err := i.invoke(ctx, &splitterprivatepb.PlacementRequest{
		Req: &splitterprivatepb.PlacementRequest_List{
			List: request,
		},
	})
	if err != nil {
		return nil, model.ToGRPCError(err)
	}
	return resp.GetList(), err
}

func (i *InternalPlacementService) New(ctx context.Context, request *splitterprivatepb.NewPlacementRequest) (*splitterprivatepb.NewPlacementResponse, error) {
	if _, err := model.ParseQualifiedPlacementName(request.GetName()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if _, err := core.ParseInternalPlacementConfig(request.GetConfig()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	resp, err := i.invoke(ctx, &splitterprivatepb.PlacementRequest{
		Req: &splitterprivatepb.PlacementRequest_New{
			New: request,
		},
	})
	if err != nil {
		return nil, model.ToGRPCError(err)
	}
	return resp.GetNew(), err
}

func (i *InternalPlacementService) Info(ctx context.Context, request *splitterprivatepb.InfoPlacementRequest) (*splitterprivatepb.InfoPlacementResponse, error) {
	if _, err := model.ParseQualifiedPlacementName(request.GetName()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	resp, err := i.invoke(ctx, &splitterprivatepb.PlacementRequest{
		Req: &splitterprivatepb.PlacementRequest_Info{
			Info: request,
		},
	})
	if err != nil {
		return nil, model.ToGRPCError(err)
	}
	return resp.GetInfo(), err
}

func (i *InternalPlacementService) Update(ctx context.Context, request *splitterprivatepb.UpdatePlacementRequest) (*splitterprivatepb.UpdatePlacementResponse, error) {
	if _, err := model.ParseQualifiedPlacementName(request.GetName()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	resp, err := i.invoke(ctx, &splitterprivatepb.PlacementRequest{
		Req: &splitterprivatepb.PlacementRequest_Update{
			Update: request,
		},
	})
	if err != nil {
		return nil, model.ToGRPCError(err)
	}
	return resp.GetUpdate(), err
}

func (i *InternalPlacementService) Delete(ctx context.Context, request *splitterprivatepb.DeletePlacementRequest) (*splitterprivatepb.DeletePlacementResponse, error) {
	if _, err := model.ParseQualifiedPlacementName(request.GetName()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	resp, err := i.invoke(ctx, &splitterprivatepb.PlacementRequest{
		Req: &splitterprivatepb.PlacementRequest_Delete{
			Delete: request,
		},
	})
	if err != nil {
		return nil, model.ToGRPCError(err)
	}
	return resp.GetDelete(), err
}

func (i *InternalPlacementService) invoke(ctx context.Context, request *splitterprivatepb.PlacementRequest) (*splitterprivatepb.PlacementResponse, error) {
	req := leader.NewHandlePlacementRequest(request)

	resp, err := model.RetryOwnership1(ctx, handleTimeout, func(ctx context.Context) (*splitterprivatepb.LeaderHandleResponse, error) {
		return core.InvokeZero(ctx, i.resolver, splitterprivatepb.LeaderServiceClient.Handle, req.Proto, func() (*splitterprivatepb.LeaderHandleResponse, error) {
			return i.proxy.Handle(ctx, req)
		})
	})

	if err != nil {
		log.Errorf(ctx, "Invoke %v failed: %v", req, err)
		return nil, err
	}
	return resp.GetPlacement(), nil
}
