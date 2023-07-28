package frontend

import (
	"context"
	"atoms.co/lib-go/pkg/clock"
	"go.atoms.co/lib/mapx"
	"go.atoms.co/slicex"
	"go.atoms.co/splitter/pkg/core"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pkg/storage"
	"go.atoms.co/splitter/pkg/storage/memdb"
	"go.atoms.co/splitter/pb/private"
	"go.atoms.co/splitter/pb"
	"fmt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"time"
)

// TODO(herohde) 7/27/2023: redirect all calls to leader instead

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
	db storage.Placements // replace with leader client
}

func NewInternalPlacementService() *InternalPlacementService {
	_, db := memdb.New(clock.New())
	return &InternalPlacementService{db: db.Placements}
}

func (i *InternalPlacementService) List(ctx context.Context, request *internal_v1.ListPlacementsRequest) (*internal_v1.ListPlacementsResponse, error) {
	tenant := model.TenantName(request.GetTenant())

	list, err := i.db.List(ctx, tenant)
	if err != nil {
		return nil, model.WrapError(err)
	}
	return &internal_v1.ListPlacementsResponse{
		Info: slicex.Map(list, core.UnwrapInternalPlacementInfo),
	}, nil

}

func (i *InternalPlacementService) New(ctx context.Context, request *internal_v1.NewPlacementRequest) (*internal_v1.NewPlacementResponse, error) {
	name, err := model.ParseQualifiedPlacementName(request.GetName())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	cfg, err := core.ParseInternalPlacementConfig(request.GetConfig())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	ret, err := i.db.Create(ctx, core.NewInternalPlacement(name, cfg, time.Now()))
	if err != nil {
		return nil, model.WrapError(err)
	}
	return &internal_v1.NewPlacementResponse{
		Info: core.UnwrapInternalPlacementInfo(ret),
	}, nil
}

func (i *InternalPlacementService) Info(ctx context.Context, request *internal_v1.InfoPlacementRequest) (*internal_v1.InfoPlacementResponse, error) {
	name, err := model.ParseQualifiedPlacementName(request.GetName())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	ret, err := i.db.Read(ctx, name)
	if err != nil {
		return nil, model.WrapError(err)
	}
	return &internal_v1.InfoPlacementResponse{
		Info: core.UnwrapInternalPlacementInfo(ret),
	}, nil
}

func (i *InternalPlacementService) Update(ctx context.Context, request *internal_v1.UpdatePlacementRequest) (*internal_v1.UpdatePlacementResponse, error) {
	return nil, model.WrapError(fmt.Errorf("not implemented"))
}

func (i *InternalPlacementService) Delete(ctx context.Context, request *internal_v1.DeletePlacementRequest) (*internal_v1.DeletePlacementResponse, error) {
	name, err := model.ParseQualifiedPlacementName(request.GetName())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	if err := i.db.Delete(ctx, name); err != nil {
		return nil, model.WrapError(err)
	}
	return &internal_v1.DeletePlacementResponse{}, nil
}
