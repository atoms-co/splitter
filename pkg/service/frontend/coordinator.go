package frontend

import (
	"context"
	"atoms.co/lib-go/pkg/clock"
	"go.atoms.co/splitter/lib/service/session"
	"go.atoms.co/lib/log"
	"go.atoms.co/lib/chanx"
	"go.atoms.co/lib/contextx"
	"go.atoms.co/lib/net/grpcx"
	"go.atoms.co/splitter/pkg/core"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pkg/service/coordinator"
	"go.atoms.co/splitter/pkg/service/worker"
	"go.atoms.co/splitter/pb/private"
	"errors"
	"fmt"
	"time"
)

// CoordinatorService is a grpc frontend for the internal coordinator api.
type CoordinatorService struct {
	cl       clock.Clock
	worker   *worker.Worker
	resolver core.ServiceResolver
}

func NewCoordinatorService(cl clock.Clock, worker *worker.Worker, resolver core.ServiceResolver) *CoordinatorService {
	c := CoordinatorService{
		cl:       cl,
		worker:   worker,
		resolver: resolver,
	}
	return &c
}

func (c *CoordinatorService) Connect(server internal_v1.CoordinatorService_ConnectServer) error {
	sess, out := session.NewServer(server.Context(), c.cl)
	defer sess.Close()
	wctx, _ := contextx.WithQuitCancel(server.Context(), sess.Closed()) // cancel context if session server closes

	return grpcx.Receive(wctx, server, func(ctx context.Context, in <-chan *internal_v1.ConnectMessage) (<-chan *internal_v1.ConnectMessage, error) {
		ch := chanx.MapIf(in, func(pb *internal_v1.ConnectMessage) (model.ConsumerMessage, bool) {
			if pb.GetSession() != nil {
				sess.Observe(ctx, session.WrapMessage(pb.GetSession())) // inject into session client
			}
			return model.WrapConsumerMessage(pb.GetConsumer()), true
		})

		// Read session initialization message

		establish, ok := chanx.TryRead(sess.Establish(), 20*time.Second)
		if !ok {
			log.Errorf(ctx, "No session establish message received")
			return nil, model.WrapError(fmt.Errorf("no session establish message: %w", model.ErrInvalid))
		}

		register, err := tryReadRegister(ctx, ch)
		if err != nil {
			return nil, model.WrapError(err)
		}

		// Should be local, model.ErrNoResolution indicates a local coordinator
		cc, err := c.resolver.Resolve(ctx, register.Service())
		if !errors.Is(err, model.ErrNoResolution) {
			log.Debugf(ctx, "Service %v was not local: %v", register.Service(), err)
			return nil, model.WrapError(err)
		}

		// Let worker handle connect

		resp, err := c.worker.Connect(ctx, establish.ID, cc.GID, register, ch)
		if err != nil {
			log.Errorf(ctx, "Connect rejected: %v", err)
			return nil, model.WrapError(err)
		}

		joined := session.Receive(sess, chanx.Map(resp, coordinator.NewConnectConsumerMessage), out, coordinator.NewConnectSessionMessage)
		return chanx.Map(joined, coordinator.UnwrapConnectMessage), nil
	})
}
