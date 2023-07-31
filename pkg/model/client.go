package model

import (
	"context"
	"go.atoms.co/slicex"
	"go.atoms.co/splitter/pb/private"
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

type Client interface {
	RaftJoin(ctx context.Context, addr, id string) error
}

type client struct {
	raft internal_v1.RaftServiceClient
}

func (c client) RaftJoin(ctx context.Context, id, addr string) error {
	req := &internal_v1.RaftJoinRequest{
		Id:   id,
		Addr: addr,
	}
	_, err := c.raft.Join(ctx, req)
	return err
}

func NewClient(cc *grpc.ClientConn) Client {
	return &client{
		raft: internal_v1.NewRaftServiceClient(cc),
	}
}
