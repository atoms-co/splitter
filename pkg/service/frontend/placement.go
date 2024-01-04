package frontend

import (
	"context"
	"go.atoms.co/lib/log"
	"go.atoms.co/slicex"
	"go.atoms.co/splitter/pkg/core"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pkg/service/leader"
	"go.atoms.co/splitter/pb/private"
	"go.atoms.co/splitter/pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"time"
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

func (p *PlacementService) List(ctx context.Context, request *public_v1.ListPlacementsRequest) (*public_v1.ListPlacementsResponse, error) {
	resp, err := p.internal.List(ctx, &internal_v1.ListPlacementsRequest{Tenant: request.GetTenant()})
	if err != nil {
		return nil, err
	}

	return &public_v1.ListPlacementsResponse{
		Info: slicex.Map(resp.GetInfo(), func(t *internal_v1.InternalPlacementInfo) *public_v1.PlacementInfo {
			return model.UnwrapPlacementInfo(core.WrapInternalPlacementInfo(t).ToPlacementInfo())
		}),
	}, nil
}

func (p *PlacementService) Info(ctx context.Context, request *public_v1.InfoPlacementRequest) (*public_v1.InfoPlacementResponse, error) {
	resp, err := p.internal.Info(ctx, &internal_v1.InfoPlacementRequest{Name: request.GetName()})
	if err != nil {
		return nil, err
	}
	return &public_v1.InfoPlacementResponse{
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

func (i *InternalPlacementService) List(ctx context.Context, request *internal_v1.ListPlacementsRequest) (*internal_v1.ListPlacementsResponse, error) {
	if request.GetTenant() == "" {
		return nil, status.Error(codes.InvalidArgument, "empty tenant")
	}

	resp, err := i.invoke(ctx, &internal_v1.PlacementRequest{
		Req: &internal_v1.PlacementRequest_List{
			List: request,
		},
	})
	return resp.GetList(), model.WrapError(err)
}

func (i *InternalPlacementService) New(ctx context.Context, request *internal_v1.NewPlacementRequest) (*internal_v1.NewPlacementResponse, error) {
	if _, err := model.ParseQualifiedPlacementName(request.GetName()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if _, err := core.ParseInternalPlacementConfig(request.GetConfig()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	resp, err := i.invoke(ctx, &internal_v1.PlacementRequest{
		Req: &internal_v1.PlacementRequest_New{
			New: request,
		},
	})
	return resp.GetNew(), model.WrapError(err)
}

func (i *InternalPlacementService) Info(ctx context.Context, request *internal_v1.InfoPlacementRequest) (*internal_v1.InfoPlacementResponse, error) {
	if _, err := model.ParseQualifiedPlacementName(request.GetName()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	resp, err := i.invoke(ctx, &internal_v1.PlacementRequest{
		Req: &internal_v1.PlacementRequest_Info{
			Info: request,
		},
	})
	return resp.GetInfo(), model.WrapError(err)
}

func (i *InternalPlacementService) Update(ctx context.Context, request *internal_v1.UpdatePlacementRequest) (*internal_v1.UpdatePlacementResponse, error) {
	if _, err := model.ParseQualifiedPlacementName(request.GetName()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	resp, err := i.invoke(ctx, &internal_v1.PlacementRequest{
		Req: &internal_v1.PlacementRequest_Update{
			Update: request,
		},
	})
	return resp.GetUpdate(), model.WrapError(err)
}

func (i *InternalPlacementService) Delete(ctx context.Context, request *internal_v1.DeletePlacementRequest) (*internal_v1.DeletePlacementResponse, error) {
	if _, err := model.ParseQualifiedPlacementName(request.GetName()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	resp, err := i.invoke(ctx, &internal_v1.PlacementRequest{
		Req: &internal_v1.PlacementRequest_Delete{
			Delete: request,
		},
	})
	return resp.GetDelete(), model.WrapError(err)
}

func (i *InternalPlacementService) invoke(ctx context.Context, request *internal_v1.PlacementRequest) (*internal_v1.PlacementResponse, error) {
	req := leader.NewHandlePlacementRequest(request)

	resp, err := model.RetryOwnership1(ctx, handleTimeout, func(ctx context.Context) (*internal_v1.LeaderHandleResponse, error) {
		return model.InvokeExZero(ctx, i.resolver, internal_v1.LeaderServiceClient.Handle, req.Proto, func() (*internal_v1.LeaderHandleResponse, error) {
			return i.proxy.Handle(ctx, req)
		})
	})

	if err != nil {
		log.Errorf(ctx, "Invoke %v failed: %v", req, err)
		return nil, model.WrapError(err)
	}
	return resp.GetPlacement(), nil
}
