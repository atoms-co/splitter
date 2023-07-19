// Package server is the Splitter server.
package server

import (
	"context"
	"atoms.co/lib-go/pkg/clock"
	"go.atoms.co/splitter/lib/service/location"
	"go.atoms.co/lib/log"
	"go.atoms.co/lib/metrics"
	"go.atoms.co/lib/statshandlerx"
	"go.atoms.co/splitter/pkg/service/frontend"
	"go.atoms.co/splitter/pb/private"
	"go.atoms.co/splitter/pb"
	"google.golang.org/grpc"
	"net"
	"sync"
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
}

func New(ctx context.Context, cl clock.Clock, loc location.Location, instance, endpoint string, opts ...Option) *Server {
	var opt options
	for _, fn := range opts {
		fn(&opt)
	}

	return &Server{
		cl: cl,
	}
}

// Serve starts the public grpc server on the given port. Blocking.
func (s *Server) Serve(ctx context.Context, listener net.Listener) error {
	gs := grpc.NewServer(metrics.WithGrpcStatsHandler(), statshandlerx.WithServerGRPCStatsHandler())
	public_v1.RegisterManagementServiceServer(gs, frontend.NewManagementService())

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()

		<-ctx.Done()
		gs.GracefulStop()
	}()

	if err := gs.Serve(listener); err != nil {
		return err
	}

	wg.Wait()

	return nil
}

// ServeInternal starts the internal grpc server on the given port. Blocking.
func (s *Server) ServeInternal(ctx context.Context, listener net.Listener) error {
	gs := grpc.NewServer(metrics.WithGrpcStatsHandler(), statshandlerx.WithServerGRPCStatsHandler())
	internal_v1.RegisterLeaderServiceServer(gs, frontend.NewLeaderService(s.cl))

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()

		<-ctx.Done()
		gs.GracefulStop()
	}()

	if err := gs.Serve(listener); err != nil {
		return err
	}

	wg.Wait()

	return nil
}

// Shutdown tries to gracefully shut down the instance components.
func (s *Server) Shutdown(ctx context.Context, timeout time.Duration) {
	log.Infof(ctx, "Shutting down server")
	now := time.Now()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-timer.C:
		log.Warnf(ctx, "Failed to drain leader in %v", time.Since(now))
	}
}
