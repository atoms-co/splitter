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

	CoordinatorInfo(ctx context.Context, service model.QualifiedServiceName) ([]model.Consumer, model.ClusterSnapshot, error)
	CoordinatorRestart(ctx context.Context, name model.QualifiedServiceName) error
	CoordinatorClusterSync(ctx context.Context, name model.QualifiedServiceName) error
	ConsumerSuspend(ctx context.Context, name model.QualifiedServiceName, id model.InstanceID) error
	ConsumerResume(ctx context.Context, name model.QualifiedServiceName, id model.InstanceID) error
	ConsumerDrain(ctx context.Context, name model.QualifiedServiceName, id model.InstanceID) error

	RaftInfo(ctx context.Context) (map[string]string, error)
	Snapshot(ctx context.Context) (Snapshot, error)
	Restore(ctx context.Context, nuke bool) (Snapshot, error)
	RestoreFromSnapshot(ctx context.Context, snapshot Snapshot) (Snapshot, error)
}

type client struct {
	placement internal_v1.PlacementManagementServiceClient
	operation internal_v1.OperationServiceClient
}

func NewClient(cc *grpc.ClientConn) Client {
	return &client{
		placement: internal_v1.NewPlacementManagementServiceClient(cc),
		operation: internal_v1.NewOperationServiceClient(cc),
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
		Name:   name.ToProto(),
		Config: UnwrapInternalPlacementConfig(NewInternalPlacementConfig(target, target, 1)),
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

func (c *client) CoordinatorInfo(ctx context.Context, service model.QualifiedServiceName) ([]model.Consumer, model.ClusterSnapshot, error) {
	req := &internal_v1.CoordinatorInfoRequest{
		Service: service.ToProto(),
	}

	resp, err := c.operation.CoordinatorInfo(ctx, req)
	if err != nil {
		return nil, model.ClusterSnapshot{}, err
	}
	return slicex.Map(resp.GetConsumers(), model.WrapInstance), model.WrapClusterSnapshot(resp.GetSnapshot()), nil
}

func (c *client) CoordinatorRestart(ctx context.Context, service model.QualifiedServiceName) error {
	req := &internal_v1.CoordinatorRestartRequest{
		Service: service.ToProto(),
	}

	_, err := c.operation.CoordinatorRestart(ctx, req)
	return err
}

func (c *client) CoordinatorClusterSync(ctx context.Context, service model.QualifiedServiceName) error {
	req := &internal_v1.CoordinatorClusterSyncRequest{
		Service: service.ToProto(),
	}

	_, err := c.operation.CoordinatorClusterSync(ctx, req)
	return err
}

func (c *client) ConsumerSuspend(ctx context.Context, service model.QualifiedServiceName, id model.InstanceID) error {
	req := &internal_v1.ConsumerSuspendRequest{
		Service:    service.ToProto(),
		ConsumerId: string(id),
	}

	_, err := c.operation.ConsumerSuspend(ctx, req)
	return err
}

func (c *client) ConsumerResume(ctx context.Context, service model.QualifiedServiceName, id model.InstanceID) error {
	req := &internal_v1.ConsumerResumeRequest{
		Service:    service.ToProto(),
		ConsumerId: string(id),
	}

	_, err := c.operation.ConsumerResume(ctx, req)
	return err
}

func (c *client) ConsumerDrain(ctx context.Context, service model.QualifiedServiceName, id model.InstanceID) error {
	req := &internal_v1.ConsumerDrainRequest{
		Service:    service.ToProto(),
		ConsumerId: string(id),
	}

	_, err := c.operation.ConsumerDrain(ctx, req)
	return err
}

func (c *client) RaftInfo(ctx context.Context) (map[string]string, error) {
	req := &internal_v1.RaftInfoRequest{}

	resp, err := c.operation.RaftInfo(ctx, req)
	return resp.GetRaftState(), err
}

func (c *client) Snapshot(ctx context.Context) (Snapshot, error) {
	req := &internal_v1.SnapshotRequest{}

	resp, err := c.operation.Snapshot(ctx, req)
	return WrapSnapshot(resp.GetSnapshot()), err
}

func (c *client) Restore(ctx context.Context, nuke bool) (Snapshot, error) {
	req := &internal_v1.RestoreRequest{
		Nuke: nuke,
	}

	resp, err := c.operation.Restore(ctx, req)
	return WrapSnapshot(resp.GetSnapshot()), err
}

func (c *client) RestoreFromSnapshot(ctx context.Context, snapshot Snapshot) (Snapshot, error) {
	req := &internal_v1.RestoreRequest{
		Snapshot: UnwrapSnapshot(snapshot),
	}

	resp, err := c.operation.Restore(ctx, req)
	return WrapSnapshot(resp.GetSnapshot()), err
}
