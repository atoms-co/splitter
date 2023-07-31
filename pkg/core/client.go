package core

import (
	"context"
	"go.atoms.co/slicex"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pb/private"
	"google.golang.org/grpc"
)

type UpdatePlacementOption func(req *internal_v1.UpdatePlacementRequest)

func WithPlacementState(state PlacementState) UpdatePlacementOption {
	return func(req *internal_v1.UpdatePlacementRequest) {
		req.State = state
	}
}

func WithPlacementConfig(cfg InternalPlacementConfig) UpdatePlacementOption {
	return func(req *internal_v1.UpdatePlacementRequest) {
		req.Config = UnwrapInternalPlacementConfig(cfg)
	}
}

// Client is the internal client.
type Client interface {
	ListPlacements(ctx context.Context, tenant model.TenantName) ([]InternalPlacementInfo, error)
	NewPlacement(ctx context.Context, name model.QualifiedPlacementName, target BlockDistribution) (InternalPlacementInfo, error)
	InfoPlacement(ctx context.Context, name model.QualifiedPlacementName) (InternalPlacementInfo, error)
	UpdatePlacement(ctx context.Context, name model.QualifiedPlacementName, guard model.Version, opts ...UpdatePlacementOption) (InternalPlacementInfo, error)
	DeletePlacement(ctx context.Context, name model.QualifiedPlacementName) error

	RaftJoin(ctx context.Context, addr, id string) error
}

type client struct {
	placement internal_v1.PlacementManagementServiceClient
	raft      internal_v1.RaftServiceClient
}

func NewClient(cc *grpc.ClientConn) Client {
	return &client{
		placement: internal_v1.NewPlacementManagementServiceClient(cc),
		raft:      internal_v1.NewRaftServiceClient(cc),
	}
}

func (c *client) ListPlacements(ctx context.Context, tenant model.TenantName) ([]InternalPlacementInfo, error) {
	req := &internal_v1.ListPlacementsRequest{
		Tenant: string(tenant),
	}
	resp, err := c.placement.List(ctx, req)
	if err != nil {
		return nil, err
	}
	return slicex.Map(resp.GetInfo(), WrapInternalPlacementInfo), nil
}

func (c *client) NewPlacement(ctx context.Context, name model.QualifiedPlacementName, target BlockDistribution) (InternalPlacementInfo, error) {
	req := &internal_v1.NewPlacementRequest{
		Name: name.ToProto(),
		Config: &internal_v1.InternalPlacement_Config{
			Target:  UnwrapBlockDistribution(target),
			Current: UnwrapBlockDistribution(target),
		},
	}
	resp, err := c.placement.New(ctx, req)
	if err != nil {
		return InternalPlacementInfo{}, err
	}
	return WrapInternalPlacementInfo(resp.GetInfo()), nil

}

func (c *client) InfoPlacement(ctx context.Context, name model.QualifiedPlacementName) (InternalPlacementInfo, error) {
	req := &internal_v1.InfoPlacementRequest{
		Name: name.ToProto(),
	}
	resp, err := c.placement.Info(ctx, req)
	if err != nil {
		return InternalPlacementInfo{}, err
	}
	return WrapInternalPlacementInfo(resp.GetInfo()), nil
}

func (c *client) UpdatePlacement(ctx context.Context, name model.QualifiedPlacementName, guard model.Version, opts ...UpdatePlacementOption) (InternalPlacementInfo, error) {
	req := &internal_v1.UpdatePlacementRequest{
		Name:    name.ToProto(),
		Version: int64(guard),
	}
	for _, fn := range opts {
		fn(req)
	}

	resp, err := c.placement.Update(ctx, req)
	if err != nil {
		return InternalPlacementInfo{}, err
	}
	return WrapInternalPlacementInfo(resp.GetInfo()), nil
}

func (c *client) DeletePlacement(ctx context.Context, name model.QualifiedPlacementName) error {
	req := &internal_v1.DeletePlacementRequest{
		Name: name.ToProto(),
	}
	_, err := c.placement.Delete(ctx, req)
	return err
}

func (c client) RaftJoin(ctx context.Context, id, addr string) error {
	req := &internal_v1.RaftJoinRequest{
		Id:   id,
		Addr: addr,
	}
	_, err := c.raft.Join(ctx, req)
	return err
}
