package core

import (
	"context"

	"google.golang.org/grpc"

	"go.atoms.co/slicex"
	"go.atoms.co/lib/stringx"
	"go.atoms.co/splitter/pkg/model"
	splitterprivatepb "go.atoms.co/splitter/pb/private"
)

type UpdatePlacementOption func(req *splitterprivatepb.UpdatePlacementRequest)

func WithPlacementState(state PlacementState) UpdatePlacementOption {
	return func(req *splitterprivatepb.UpdatePlacementRequest) {
		req.State = state
	}
}

func WithPlacementConfig(cfg InternalPlacementConfig) UpdatePlacementOption {
	return func(req *splitterprivatepb.UpdatePlacementRequest) {
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
	CoordinatorRevokeGrants(ctx context.Context, service model.QualifiedServiceName, grants map[model.ConsumerID][]GrantID) error
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
	placement splitterprivatepb.PlacementManagementServiceClient
	operation splitterprivatepb.OperationServiceClient
}

func NewClient(cc *grpc.ClientConn) Client {
	return &client{
		placement: splitterprivatepb.NewPlacementManagementServiceClient(cc),
		operation: splitterprivatepb.NewOperationServiceClient(cc),
	}
}

func (c *client) ListPlacements(ctx context.Context, tenant model.TenantName) ([]InternalPlacementInfo, error) {
	req := &splitterprivatepb.ListPlacementsRequest{
		Tenant: string(tenant),
	}
	resp, err := c.placement.List(ctx, req)
	if err != nil {
		return nil, err
	}
	return slicex.Map(resp.GetInfo(), WrapInternalPlacementInfo), nil
}

func (c *client) NewPlacement(ctx context.Context, name model.QualifiedPlacementName, target BlockDistribution) (InternalPlacementInfo, error) {
	req := &splitterprivatepb.NewPlacementRequest{
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
	req := &splitterprivatepb.InfoPlacementRequest{
		Name: name.ToProto(),
	}
	resp, err := c.placement.Info(ctx, req)
	if err != nil {
		return InternalPlacementInfo{}, err
	}
	return WrapInternalPlacementInfo(resp.GetInfo()), nil
}

func (c *client) UpdatePlacement(ctx context.Context, name model.QualifiedPlacementName, guard model.Version, opts ...UpdatePlacementOption) (InternalPlacementInfo, error) {
	req := &splitterprivatepb.UpdatePlacementRequest{
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
	req := &splitterprivatepb.DeletePlacementRequest{
		Name: name.ToProto(),
	}
	_, err := c.placement.Delete(ctx, req)
	return err
}

func (c *client) CoordinatorInfo(ctx context.Context, service model.QualifiedServiceName) ([]model.Consumer, model.ClusterSnapshot, error) {
	req := &splitterprivatepb.CoordinatorInfoRequest{
		Service: service.ToProto(),
	}

	resp, err := c.operation.CoordinatorInfo(ctx, req)
	if err != nil {
		return nil, model.ClusterSnapshot{}, err
	}
	return slicex.Map(resp.GetConsumers(), model.WrapInstance), model.WrapClusterSnapshot(resp.GetSnapshot()), nil
}

func (c *client) CoordinatorRestart(ctx context.Context, service model.QualifiedServiceName) error {
	req := &splitterprivatepb.CoordinatorRestartRequest{
		Service: service.ToProto(),
	}

	_, err := c.operation.CoordinatorRestart(ctx, req)
	return err
}

func (c *client) CoordinatorRevokeGrants(ctx context.Context, service model.QualifiedServiceName, grants map[model.ConsumerID][]model.GrantID) error {
	var pbs []*splitterprivatepb.CoordinatorRevokeGrantsRequest_ConsumerGrants
	for cid, gs := range grants {
		pbs = append(pbs, &splitterprivatepb.CoordinatorRevokeGrantsRequest_ConsumerGrants{
			Consumer: string(cid),
			Grants:   slicex.Map(gs, stringx.ToString[model.GrantID]),
		})
	}

	req := &splitterprivatepb.CoordinatorRevokeGrantsRequest{
		Service: service.ToProto(),
		Grants:  pbs,
	}

	_, err := c.operation.CoordinatorRevokeGrants(ctx, req)
	return err
}

func (c *client) CoordinatorClusterSync(ctx context.Context, service model.QualifiedServiceName) error {
	req := &splitterprivatepb.CoordinatorClusterSyncRequest{
		Service: service.ToProto(),
	}

	_, err := c.operation.CoordinatorClusterSync(ctx, req)
	return err
}

func (c *client) ConsumerSuspend(ctx context.Context, service model.QualifiedServiceName, id model.InstanceID) error {
	req := &splitterprivatepb.ConsumerSuspendRequest{
		Service:    service.ToProto(),
		ConsumerId: string(id),
	}

	_, err := c.operation.ConsumerSuspend(ctx, req)
	return err
}

func (c *client) ConsumerResume(ctx context.Context, service model.QualifiedServiceName, id model.InstanceID) error {
	req := &splitterprivatepb.ConsumerResumeRequest{
		Service:    service.ToProto(),
		ConsumerId: string(id),
	}

	_, err := c.operation.ConsumerResume(ctx, req)
	return err
}

func (c *client) ConsumerDrain(ctx context.Context, service model.QualifiedServiceName, id model.InstanceID) error {
	req := &splitterprivatepb.ConsumerDrainRequest{
		Service:    service.ToProto(),
		ConsumerId: string(id),
	}

	_, err := c.operation.ConsumerDrain(ctx, req)
	return err
}

func (c *client) RaftInfo(ctx context.Context) (map[string]string, error) {
	req := &splitterprivatepb.RaftInfoRequest{}

	resp, err := c.operation.RaftInfo(ctx, req)
	return resp.GetRaftState(), err
}

func (c *client) Snapshot(ctx context.Context) (Snapshot, error) {
	req := &splitterprivatepb.SnapshotRequest{}

	resp, err := c.operation.Snapshot(ctx, req)
	return WrapSnapshot(resp.GetSnapshot()), err
}

func (c *client) Restore(ctx context.Context, nuke bool) (Snapshot, error) {
	req := &splitterprivatepb.RestoreRequest{
		Nuke: nuke,
	}

	resp, err := c.operation.Restore(ctx, req)
	return WrapSnapshot(resp.GetSnapshot()), err
}

func (c *client) RestoreFromSnapshot(ctx context.Context, snapshot Snapshot) (Snapshot, error) {
	req := &splitterprivatepb.RestoreRequest{
		Snapshot: UnwrapSnapshot(snapshot),
	}

	resp, err := c.operation.Restore(ctx, req)
	return WrapSnapshot(resp.GetSnapshot()), err
}
