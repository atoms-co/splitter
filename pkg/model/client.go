package model

import (
	"context"
	"go.atoms.co/slicex"
	"go.atoms.co/splitter/pb"
	"google.golang.org/grpc"
)

// PlacementClient is a client for querying placement information.
type PlacementClient interface {
	List(ctx context.Context, name TenantName) ([]PlacementInfo, error)
	Info(ctx context.Context, name QualifiedPlacementName) (PlacementInfo, error)
}

type placementClient struct {
	client public_v1.PlacementServiceClient
}

func NewPlacementClient(cc *grpc.ClientConn) PlacementClient {
	return &placementClient{
		client: public_v1.NewPlacementServiceClient(cc),
	}
}

func (p *placementClient) List(ctx context.Context, name TenantName) ([]PlacementInfo, error) {
	req := &public_v1.ListPlacementsRequest{
		Tenant: string(name),
	}
	resp, err := p.client.List(ctx, req)
	if err != nil {
		return nil, err
	}
	return slicex.Map(resp.GetInfo(), WrapPlacementInfo), nil
}

func (p *placementClient) Info(ctx context.Context, name QualifiedPlacementName) (PlacementInfo, error) {
	req := &public_v1.InfoPlacementRequest{
		Name: name.ToProto(),
	}
	resp, err := p.client.Info(ctx, req)
	if err != nil {
		return PlacementInfo{}, err
	}
	return WrapPlacementInfo(resp.GetInfo()), nil

}
