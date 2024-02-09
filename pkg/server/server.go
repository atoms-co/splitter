// Package server is the Splitter server.
package server

import (
	"context"
	"atoms.co/lib-go/pkg/clock"
	"go.atoms.co/splitter/lib/service/location"
	"go.atoms.co/splitter/lib/service/session"
	"go.atoms.co/lib/log"
	"go.atoms.co/lib/chanx"
	"go.atoms.co/lib/contextx"
	"go.atoms.co/lib/net/grpcx"
	"go.atoms.co/lib/statshandlerx"
	"go.atoms.co/splitter/pkg/cluster"
	"go.atoms.co/splitter/pkg/core"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pkg/service/coordinator"
	"go.atoms.co/splitter/pkg/service/frontend"
	"go.atoms.co/splitter/pkg/service/leader"
	"go.atoms.co/splitter/pkg/service/worker"
	"go.atoms.co/splitter/pb/private"
	"go.atoms.co/splitter/pb"
	"google.golang.org/grpc"
	"net"
	"time"
)

type options struct {
	fastActivation bool
}

// Option is a server option.
type Option func(*options)

// WithFastActivation true skips the recovery period and lets the leader allocate existing shards immediately. Unsafe.
func WithFastActivation(fastActivation bool) Option {
	return func(o *options) {
		o.fastActivation = fastActivation
	}
}

// Server holds all service components.
type Server struct {
	cl  clock.Clock
	loc location.Location

	cluster  *cluster.Cluster
	manager  *leader.Manager
	resolver core.ServiceResolver
	worker   *worker.Worker
}

func New(ctx context.Context, cl clock.Clock, loc location.Location, endpoint string, cluster *cluster.Cluster, manager *leader.Manager, opts ...Option) *Server {
	var opt options
	for _, fn := range opts {
		fn(&opt)
	}

	joinFn := func(ctx context.Context, self location.Instance, handler grpcx.Handler[leader.Message, leader.Message]) error {
		client, err := manager.Resolve(ctx, model.ZeroDomainKey)
		if err != nil {
			return grpcx.ShortCircuit(ctx, handler, func(ctx context.Context, in <-chan leader.Message) (<-chan leader.Message, error) {
				return manager.Join(ctx, session.NewID(), in)
			})
		}

		sess, establish, out := session.NewClient(ctx, cl, self)
		defer sess.Close()
		wctx, _ := contextx.WithQuitCancel(ctx, sess.Closed()) // cancel context if session client closes

		return grpcx.Connect(wctx, client.Join, func(ctx context.Context, in <-chan *internal_v1.JoinMessage) (<-chan *internal_v1.JoinMessage, error) {
			ch := chanx.MapIf(in, func(pb *internal_v1.JoinMessage) (leader.Message, bool) {
				if pb.GetSession() != nil {
					sess.Observe(ctx, session.WrapMessage(pb.GetSession())) // inject into session client
					return leader.Message{}, false
				}
				return leader.WrapMessage(pb.GetLeader()), true
			})

			resp, err := handler(ctx, ch)
			if err != nil {
				return nil, model.WrapError(err)
			}

			joined := session.Connect(sess, establish, chanx.Map(resp, leader.NewJoinMessage), out, leader.NewJoinSessionMessage)
			return chanx.Map(joined, leader.UnwrapJoinMessage), nil
		})
	}

	factoryFn := func(ctx context.Context, service model.QualifiedServiceName, state core.State, updates <-chan core.Update) coordinator.Coordinator {
		var lopts []coordinator.Option
		if opt.fastActivation {
			lopts = append(lopts, coordinator.WithFastActivation())
		}
		return coordinator.New(ctx, cl, loc, service, state, updates, lopts...)
	}

	w, out := worker.New(cl, loc, endpoint, joinFn, factoryFn)
	resolver := core.NewServiceResolver(ctx, cl, w.Self(), out, grpcx.WithInsecure())

	return &Server{
		cl:       cl,
		loc:      loc,
		cluster:  cluster,
		manager:  manager,
		resolver: resolver,
		worker:   w,
	}
}

// Serve starts the public grpc server on the given port. Blocking.
func (s *Server) Serve(ctx context.Context, listener net.Listener) error {
	placement := frontend.NewInternalPlacementService(s.manager, s.manager)

	gs := grpc.NewServer(statshandlerx.WithServerGRPCStatsHandler())
	public_v1.RegisterConsumerServiceServer(gs, frontend.NewConsumerService(s.cl, s.worker, s.resolver))
	public_v1.RegisterManagementServiceServer(gs, frontend.NewManagementService(s.manager, s.manager))
	public_v1.RegisterPlacementServiceServer(gs, frontend.NewPlacementService(placement))
	internal_v1.RegisterPlacementManagementServiceServer(gs, placement)

	return grpcx.Serve(ctx, gs, listener)
}

// ServeInternal starts the internal grpc server on the given port. Blocking.
func (s *Server) ServeInternal(ctx context.Context, listener net.Listener) error {
	gs := grpc.NewServer(statshandlerx.WithServerGRPCStatsHandler())
	internal_v1.RegisterLeaderServiceServer(gs, frontend.NewLeaderService(s.cl, s.loc, s.manager))
	internal_v1.RegisterCoordinatorServiceServer(gs, frontend.NewCoordinatorService(s.cl, s.worker))
	internal_v1.RegisterClusterServiceServer(gs, frontend.NewClusterService(s.cluster))
	internal_v1.RegisterOperationServiceServer(gs, frontend.NewOperationService(s.cluster, s.manager, s.manager))

	return grpcx.Serve(ctx, gs, listener)
}

// Shutdown tries to gracefully shut down the instance components.
func (s *Server) Shutdown(ctx context.Context, timeout time.Duration) {
	log.Infof(ctx, "Shutting down server")
	now := time.Now()

	workerDrainTimeout := time.NewTimer(timeout)
	defer workerDrainTimeout.Stop()

	s.worker.Drain(timeout)
	select {
	case <-s.worker.Closed():
		log.Infof(ctx, "Successfully drained worker in %v", time.Since(now))
	case <-workerDrainTimeout.C:
		log.Warnf(ctx, "Failed to drain worker gracefully in %v", time.Since(now))
	}

	clusterDrainTimeout := time.NewTimer(timeout)
	defer clusterDrainTimeout.Stop()

	s.cluster.Drain(timeout)
	select {
	case <-s.cluster.Closed():
		log.Infof(ctx, "Successfully drained RAFT cluster in %v", time.Since(now))
	case <-clusterDrainTimeout.C:
		log.Warnf(ctx, "Failed to drain cluster gracefully in %v", time.Since(now))
	}

	managerDrainTimeout := time.NewTimer(timeout)
	defer managerDrainTimeout.Stop()

	s.manager.Drain(timeout)
	select {
	case <-s.manager.Closed():
		log.Infof(ctx, "Successfully drained Leader manager in %v", time.Since(now))
	case <-managerDrainTimeout.C:
		log.Warnf(ctx, "Failed to drain manager gracefully in %v", time.Since(now))
	}
}
