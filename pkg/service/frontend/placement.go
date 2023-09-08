package frontend

import (
	"context"
	"go.atoms.co/lib/mapx"
	"go.atoms.co/splitter/pkg/core"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pkg/service/leader"
	"go.atoms.co/splitter/pb/private"
	"go.atoms.co/splitter/pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"time"
)

// TODO(herohde) 7/27/2023: redirect all calls to leader instead

const (
	handleTimeout = 20 * time.Second
)

type PlacementService struct {
	fixed map[model.QualifiedPlacementName]model.PlacementInfo
}

func NewPlacementService() *PlacementService {
	facility := model.QualifiedPlacementName{Tenant: "orders", Placement: "facility_id"}
	dist := model.NewDistribution("centralus", model.DistributionSplit{
		Key:    model.MustParseKey("80000000-0000-0000-0000-000000000000"),
		Region: "northcentralus",
	})

	return &PlacementService{
		fixed: map[model.QualifiedPlacementName]model.PlacementInfo{
			facility: model.NewPlacementInfo(model.NewPlacement(facility, dist), 1, time.Now()),
		},
	}
}

func (p *PlacementService) List(ctx context.Context, request *public_v1.ListPlacementsRequest) (*public_v1.ListPlacementsResponse, error) {
	tenant := model.TenantName(request.GetTenant())
	return &public_v1.ListPlacementsResponse{
		Info: mapx.MapValuesIf(p.fixed, func(v model.PlacementInfo) (*public_v1.PlacementInfo, bool) {
			if v.Placement().Name().Tenant != tenant {
				return nil, false
			}
			return model.UnwrapPlacementInfo(v), true
		}),
	}, nil
}

func (p *PlacementService) Info(ctx context.Context, request *public_v1.InfoPlacementRequest) (*public_v1.InfoPlacementResponse, error) {
	name, err := model.ParseQualifiedPlacementName(request.GetName())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	info, ok := p.fixed[name]
	if !ok {
		return nil, model.WrapError(model.ErrNotFound)
	}
	return &public_v1.InfoPlacementResponse{Info: model.UnwrapPlacementInfo(info)}, nil
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
	return resp.GetList(), err
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
	return resp.GetNew(), err
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
	return resp.GetInfo(), err
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
	return resp.GetUpdate(), err
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
	return resp.GetDelete(), err
}

func (i *InternalPlacementService) invoke(ctx context.Context, request *internal_v1.PlacementRequest) (*internal_v1.PlacementResponse, error) {
	req := leader.NewHandlePlacementRequest(request)

	resp, err := model.RetryOwnership1(ctx, handleTimeout, func(ctx context.Context) (*internal_v1.LeaderHandleResponse, error) {
		return model.InvokeEx0(ctx, i.resolver, internal_v1.LeaderServiceClient.Handle, req.Proto, func() (*internal_v1.LeaderHandleResponse, error) {
			return i.proxy.Handle(ctx, req)
		})
	})
	return resp.GetPlacement(), err
}
