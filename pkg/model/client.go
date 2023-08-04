package model

import (
	"context"
	"go.atoms.co/slicex"
	"go.atoms.co/splitter/pb"
	"google.golang.org/grpc"
)

// Client is a client for querying placement information.
type Client interface {
	ListPlacements(ctx context.Context, name TenantName) ([]PlacementInfo, error)
	InfoPlacement(ctx context.Context, name QualifiedPlacementName) (PlacementInfo, error)
}

type client struct {
	placement public_v1.PlacementServiceClient
}

func NewClient(cc *grpc.ClientConn) Client {
	return &client{
		placement: public_v1.NewPlacementServiceClient(cc),
	}
}

func (p *client) ListPlacements(ctx context.Context, name TenantName) ([]PlacementInfo, error) {
	req := &public_v1.ListPlacementsRequest{
		Tenant: string(name),
	}
	resp, err := p.placement.List(ctx, req)
	if err != nil {
		return nil, err
	}
	return slicex.Map(resp.GetInfo(), WrapPlacementInfo), nil
}

func (p *client) InfoPlacement(ctx context.Context, name QualifiedPlacementName) (PlacementInfo, error) {
	req := &public_v1.InfoPlacementRequest{
		Name: name.ToProto(),
	}
	resp, err := p.placement.Info(ctx, req)
	if err != nil {
		return PlacementInfo{}, err
	}
	return WrapPlacementInfo(resp.GetInfo()), nil
}
