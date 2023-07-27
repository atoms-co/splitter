package frontend

import (
	"context"
	"go.atoms.co/lib/mapx"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"time"
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
