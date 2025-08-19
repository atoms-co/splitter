package core

import (
	"context"
	"sync"
	"time"

	"google.golang.org/grpc"

	"atoms.co/lib-go/pkg/clock"
	"go.atoms.co/splitter/lib/service/session"
	"go.atoms.co/lib/log"
	"go.atoms.co/lib/chanx"
	"go.atoms.co/lib/contextx"
	"go.atoms.co/lib/net/grpcx"
	"go.atoms.co/lib/iox"
	"go.atoms.co/lib/randx"
	"go.atoms.co/slicex"
	"go.atoms.co/lib/stringx"
	"go.atoms.co/splitter/pkg/model"
	splitterprivatepb "go.atoms.co/splitter/pb/private"
)

type Observer = model.Instance

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

// ObserverClient usage
//
//	type ObserverService struct {
//	    splitterClient core.ObserverClient
//	    observers  map[model.QualifiedServiceName]*ServiceObserver
//	}
//
//	type ServiceObserver struct {
//	    c model.ClusterProvider
//	    closer   iox.AsyncCloser
//	}
//
//	func (s *ObserverService) getOrCreateObserver(service model.QualifiedServiceName) *ServiceObserver {
//	    observer := model.NewInstance(s.location, "foo:bar")
//	    c, updates, closer := s.splitterClient.Observe(ctx, observer, service)
//
//	    go func() {
//	        for msg := range updates {
//	            s.broadcastToClients(service, msg)
//	        }
//	    }()
//
//	    return &ServiceObserver{c: c, closer: closer}
//	}
//
//	func (s *ServiceObserver) GetCurrentState() (model.Cluster, bool) {
//	    return s.c.Cluster()
//	}
type ObserverClient interface {
	// Observe starts observing the cluster state for the specified service.
	Observe(ctx context.Context, observer Observer, service model.QualifiedServiceName) (model.ClusterProvider, <-chan model.ClusterMessage, iox.AsyncCloser)
}

type observerPool struct {
	cluster *model.ClusterMap
	updates chan model.ClusterMessage
	mu      sync.RWMutex
}

func (p *observerPool) Cluster() (model.Cluster, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.cluster, len(p.cluster.Consumers()) != 0
}

type observerClient struct {
	cl       clock.Clock
	observer splitterprivatepb.ObserverServiceClient
}

func NewObserverClient(cc *grpc.ClientConn) ObserverClient {
	return &observerClient{
		cl:       clock.New(),
		observer: splitterprivatepb.NewObserverServiceClient(cc),
	}
}

func (c *observerClient) Observe(ctx context.Context, observer Observer, service model.QualifiedServiceName) (model.ClusterProvider, <-chan model.ClusterMessage, iox.AsyncCloser) {
	pool := &observerPool{
		cluster: model.NewClusterMap(model.NewClusterID(observer.Instance(), c.cl.Now()), nil),
		updates: make(chan model.ClusterMessage, 10),
	}
	quit := iox.NewAsyncCloser()

	log.Infof(ctx, "Starting observer %v for service %v", observer.Instance(), service)

	go c.observe(ctx, observer, service, pool, quit)

	return pool, pool.updates, quit
}

func (c *observerClient) observe(ctx context.Context, observer Observer, service model.QualifiedServiceName, pool *observerPool, quit iox.AsyncCloser) {
	defer close(pool.updates)
	wctx, _ := contextx.WithQuitCancel(ctx, quit.Closed())
	iox.WithCancel(ctx, quit)

	registration := NewObserverRegisterMessage(observer, service)

	for !quit.IsClosed() {
		sess, establish, out := session.NewClient(wctx, c.cl, observer.Instance())
		sessCtx, _ := contextx.WithQuitCancel(wctx, sess.Closed())

		err := grpcx.Connect(sessCtx, c.observer.Observe, func(ctx context.Context, in <-chan *splitterprivatepb.ObserverServerMessage) (<-chan *splitterprivatepb.ObserverClientMessage, error) {
			ch := chanx.MapIf(in, func(pb *splitterprivatepb.ObserverServerMessage) (ObserverServerMessage, bool) {
				if pb.GetSession() != nil {
					sess.Observe(ctx, session.WrapMessage(pb.GetSession()))
					return ObserverServerMessage{}, false
				}
				return WrapObserverServerMessage(pb), true
			})

			resp := make(chan ObserverClientMessage, 1)
			resp <- registration
			close(resp)

			go func() {
				if err := c.processClusterUpdates(ctx, ch, pool); err != nil {
					log.Errorf(ctx, "Failed to process cluster updates: %v", err)
					sess.Close()
				}
			}()

			joined := session.Connect(sess, establish, chanx.Map(resp, UnwrapObserverClientMessage), out, func(m session.Message) *splitterprivatepb.ObserverClientMessage {
				return UnwrapObserverClientMessage(NewObserverClientMessage(m))
			})
			return joined, nil
		})

		sess.Close()

		if err != nil {
			log.Infof(wctx, "Observer %v disconnected: %v", observer.Instance(), err)
		}

		if quit.IsClosed() {
			return
		}

		c.cl.Sleep(time.Second + randx.Duration(time.Second))
	}
}

func (c *observerClient) processClusterUpdates(ctx context.Context, messages <-chan ObserverServerMessage, pool *observerPool) error {
	for msg := range messages {
		if cluster, ok := msg.ClusterMessage(); ok {
			select {
			case pool.updates <- cluster:
			case <-ctx.Done():
				return ctx.Err()
			default:
				chanx.Clear(pool.updates)
				pool.updates <- cluster
			}

			pool.mu.Lock()
			defer pool.mu.Unlock()
			updatedCluster, err := model.UpdateClusterMap(ctx, pool.cluster, cluster)
			if err != nil {
				log.Errorf(ctx, "Failed to update cluster: %v", err)
				return err
			}
			pool.cluster = updatedCluster
		}
	}
	return nil
}
