// Package server is the Splitter server.
package server

import (
	"context"
	"atoms.co/lib-go/pkg/clock"
	"go.atoms.co/splitter/lib/service/location"
	"go.atoms.co/lib/log"
	"go.atoms.co/lib/metrics"
	"go.atoms.co/lib/net/grpcx"
	"go.atoms.co/lib/statshandlerx"
	"go.atoms.co/splitter/pkg/cluster"
	"go.atoms.co/splitter/pkg/service/frontend"
	"go.atoms.co/splitter/pkg/storage"
	"go.atoms.co/splitter/pb/private"
	"go.atoms.co/splitter/pb"
	"google.golang.org/grpc"
	"net"
	"time"
)

type options struct {
	leaderFastActivation bool
}

// Option is a server option.
type Option func(*options)

// WithLeaderFastActivation true skips the recovery period and lets the leader allocate existing shards immediately. Unsafe.
func WithLeaderFastActivation(fastActivation bool) Option {
	return func(o *options) {
		o.leaderFastActivation = fastActivation
	}
}

// Server holds all service components.
type Server struct {
	cl clock.Clock

	location location.Location
	cluster  *cluster.Cluster
}

func New(ctx context.Context, cl clock.Clock, loc location.Location, cluster *cluster.Cluster, leaders <-chan *cluster.Leader, s storage.Storage, opts ...Option) *Server {
	var opt options
	for _, fn := range opts {
		fn(&opt)
	}

	// TODO(jhhurwitz): 08/01/2023 Use leader for forwarding
	go func() {
		for l := range leaders {
			// Initialize leader -> leader.New(s)
			log.Infof(ctx, "Leader info: %v", l)
		}
	}()

	return &Server{
		cl:       cl,
		location: loc,
		cluster:  cluster,
	}
}

// Serve starts the public grpc server on the given port. Blocking.
func (s *Server) Serve(ctx context.Context, listener net.Listener) error {
	gs := grpc.NewServer(metrics.WithGrpcStatsHandler(), statshandlerx.WithServerGRPCStatsHandler())
	public_v1.RegisterManagementServiceServer(gs, frontend.NewManagementService())
	public_v1.RegisterPlacementServiceServer(gs, frontend.NewPlacementService())
	internal_v1.RegisterPlacementManagementServiceServer(gs, frontend.NewInternalPlacementService())

	return grpcx.Serve(ctx, gs, listener)
}

// ServeInternal starts the internal grpc server on the given port. Blocking.
func (s *Server) ServeInternal(ctx context.Context, listener net.Listener) error {
	gs := grpc.NewServer(metrics.WithGrpcStatsHandler(), statshandlerx.WithServerGRPCStatsHandler())
	internal_v1.RegisterLeaderServiceServer(gs, frontend.NewLeaderService(s.cl))
	internal_v1.RegisterClusterServiceServer(gs, frontend.NewClusterService(s.cl, s.cluster))

	return grpcx.Serve(ctx, gs, listener)
}

// Shutdown tries to gracefully shut down the instance components.
func (s *Server) Shutdown(ctx context.Context, timeout time.Duration) {
	log.Infof(ctx, "Shutting down server")
	now := time.Now()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	s.cluster.Drain(timeout)

	select {
	case <-s.cluster.Closed():
		log.Infof(ctx, "Successfully drained cluster in %v", time.Since(now))
	case <-timer.C:
		log.Warnf(ctx, "Failed to drain gracefully in %v", time.Since(now))
	}
}
