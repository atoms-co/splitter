// Package server is the Splitter server.
package server

import (
	"context"
	"net"
	"sync"
	"time"

	"go.uber.org/multierr"
	"google.golang.org/grpc"

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
	"go.atoms.co/splitter/pkg/service/consumer"
	"go.atoms.co/splitter/pkg/service/coordinator"
	"go.atoms.co/splitter/pkg/service/frontend"
	"go.atoms.co/splitter/pkg/service/leader"
	"go.atoms.co/splitter/pkg/service/worker"
	splitterprivatepb "go.atoms.co/splitter/pb/private"
	splitterpb "go.atoms.co/splitter/pb"
)

type options struct {
	fastActivation bool
	refreshDelay   *time.Duration
}

// Option is a server option.
type Option func(*options)

// WithFastActivation true skips the recovery period and lets the leader allocate existing shards immediately. Unsafe.
func WithFastActivation(fastActivation bool) Option {
	return func(o *options) {
		o.fastActivation = fastActivation
	}
}

func WithAllocationRefreshDelay(delay time.Duration) Option {
	return func(c *options) {
		c.refreshDelay = &delay
	}
}

// Server holds all service components.
type Server struct {
	cl  clock.Clock
	loc location.Location

	cluster  cluster.Cluster
	worker   worker.Worker
	consumer consumer.Consumer
	manager  leader.Manager
	resolver core.ServiceResolver
}

func New(ctx context.Context, cl clock.Clock, loc location.Location, endpoint string, cluster cluster.Cluster, manager leader.Manager, opts ...Option) *Server {
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

		return grpcx.Connect(wctx, client.Join, func(ctx context.Context, in <-chan *splitterprivatepb.JoinMessage) (<-chan *splitterprivatepb.JoinMessage, error) {
			ch := chanx.MapIf(in, func(pb *splitterprivatepb.JoinMessage) (leader.Message, bool) {
				if pb.GetSession() != nil {
					sess.Observe(ctx, session.WrapMessage(pb.GetSession())) // inject into session client
					return leader.Message{}, false
				}
				return leader.WrapMessage(pb.GetLeader()), true
			})

			resp, err := handler(ctx, ch)
			if err != nil {
				return nil, model.ToGRPCError(err)
			}

			joined := session.Connect(sess, establish, chanx.Map(resp, leader.NewJoinMessage), out, leader.NewJoinSessionMessage)
			return chanx.Map(joined, leader.UnwrapJoinMessage), nil
		})
	}

	factoryFn := func(ctx context.Context, service model.QualifiedServiceName, state core.State, updates <-chan core.Update) coordinator.Coordinator {
		var copts []coordinator.Option
		if opt.fastActivation {
			copts = append(copts, coordinator.WithFastActivation())
		}
		if opt.refreshDelay != nil {
			copts = append(copts, coordinator.WithRefreshDelay(*opt.refreshDelay))
		}
		return coordinator.New(ctx, cl, loc, service, state, updates, copts...)
	}

	w, clusters := worker.New(cl, loc, endpoint, joinFn, factoryFn)
	resolver := core.NewServiceResolver(ctx, cl, w.Self().ID(), clusters, grpcx.WithInsecure())

	c := consumer.New(cl, loc, w, resolver)

	return &Server{
		cl:       cl,
		loc:      loc,
		cluster:  cluster,
		worker:   w,
		consumer: c,
		manager:  manager,
		resolver: resolver,
	}
}

// Serve starts the public grpc server on the given port. Blocking.
func (s *Server) Serve(ctx context.Context, listeners ...net.Listener) error {
	placement := frontend.NewInternalPlacementService(s.manager, s.manager)

	gs := grpc.NewServer(statshandlerx.WithServerGRPCStatsHandler())
	splitterpb.RegisterConsumerServiceServer(gs, frontend.NewConsumerService(s.cl, s.consumer, s.worker))
	splitterpb.RegisterManagementServiceServer(gs, frontend.NewManagementService(s.manager, s.manager))
	splitterpb.RegisterPlacementServiceServer(gs, frontend.NewPlacementService(placement))
	splitterprivatepb.RegisterPlacementManagementServiceServer(gs, placement)
	splitterprivatepb.RegisterOperationServiceServer(gs, frontend.NewOperationService(s.cluster, s.worker, s.resolver, s.manager, s.manager))

	var wg sync.WaitGroup
	var mu sync.Mutex
	var merr error
	for _, l := range listeners {
		listener := l
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := grpcx.Serve(ctx, gs, listener)
			mu.Lock()
			defer mu.Unlock()
			merr = multierr.Append(merr, err)
			// If one listener has died, signal the others to close also
			for _, l2 := range listeners {
				if l2 == listener {
					continue
				}
				merr = multierr.Append(merr, l2.Close())
			}
		}()
	}

	wg.Wait()
	return merr
}

// ServeInternal starts the internal grpc server on the given port. Blocking.
func (s *Server) ServeInternal(ctx context.Context, listeners ...net.Listener) error {
	gs := grpc.NewServer(statshandlerx.WithServerGRPCStatsHandler())
	splitterprivatepb.RegisterLeaderServiceServer(gs, frontend.NewLeaderService(s.cl, s.loc, s.manager))
	splitterprivatepb.RegisterCoordinatorServiceServer(gs, frontend.NewCoordinatorService(s.cl, s.worker))
	splitterprivatepb.RegisterClusterServiceServer(gs, frontend.NewClusterService(s.cluster))
	splitterprivatepb.RegisterObserverServiceServer(gs, frontend.NewObserverService(s.cl, s.worker))

	var wg sync.WaitGroup
	var mu sync.Mutex
	var merr error
	for _, l := range listeners {
		listener := l
		wg.Add(1)
		go func() {
			err := grpcx.Serve(ctx, gs, listener)
			mu.Lock()
			defer mu.Unlock()
			merr = multierr.Append(merr, err)
			// If one listener has died, signal the others to close also
			for _, l2 := range listeners {
				if l2 == listener {
					continue
				}
				merr = multierr.Append(merr, l2.Close())
			}
			wg.Done()
		}()
	}

	wg.Wait()
	return merr
}

// Shutdown tries to gracefully shut down the instance components.
func (s *Server) Shutdown(ctx context.Context, timeout time.Duration) {
	log.Infof(ctx, "Shutting down server")
	now := time.Now()

	drainTimeout := time.NewTimer(timeout)

	s.worker.Drain(timeout)
	select {
	case <-s.worker.Closed():
		log.Infof(ctx, "Successfully drained worker in %v", time.Since(now))
	case <-drainTimeout.C:
		log.Warnf(ctx, "Failed to drain worker gracefully in %v", time.Since(now))
	}

	clusterDrainTimeout := time.NewTimer(timeout)
	defer clusterDrainTimeout.Stop()

	s.cluster.Drain(timeout)
	select {
	case <-s.cluster.Closed():
		log.Infof(ctx, "Successfully drained RAFT cluster in %v", time.Since(now))
	case <-drainTimeout.C:
		log.Warnf(ctx, "Failed to drain cluster gracefully in %v", time.Since(now))
	}

	s.manager.Drain(timeout)
	select {
	case <-s.manager.Closed():
		log.Infof(ctx, "Successfully drained Leader manager in %v", time.Since(now))
	case <-drainTimeout.C:
		log.Warnf(ctx, "Failed to drain manager gracefully in %v", time.Since(now))
	}
}
