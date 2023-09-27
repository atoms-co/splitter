package frontend

import (
	"context"
	"atoms.co/lib-go/pkg/clock"
	"go.atoms.co/splitter/lib/service/session"
	"go.atoms.co/lib/log"
	"go.atoms.co/lib/chanx"
	"go.atoms.co/lib/contextx"
	"go.atoms.co/lib/net/grpcx"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pkg/service/worker"
	"go.atoms.co/splitter/pb/private"
	"go.atoms.co/splitter/pb"
	"time"
)

// CoordinatorService is a grpc frontend for the internal coordinator api.
type CoordinatorService struct {
	cl     clock.Clock
	worker *worker.Worker
}

func NewCoordinatorService(cl clock.Clock, worker *worker.Worker) *CoordinatorService {
	c := CoordinatorService{
		cl:     cl,
		worker: worker,
	}
	return &c
}

func (c *CoordinatorService) Connect(server internal_v1.CoordinatorService_ConnectServer) error {
	sess, out := session.NewServer(server.Context(), c.cl)
	defer sess.Close()
	wctx, _ := contextx.WithQuitCancel(server.Context(), sess.Closed()) // cancel context if session server closes

	return grpcx.Receive(wctx, server, func(ctx context.Context, in <-chan *public_v1.ConsumerMessage) (<-chan *public_v1.CoordinatorMessage, error) {
		ch := chanx.MapIf(in, func(pb *public_v1.ConsumerMessage) (model.ConsumerMessage, bool) {
			if pb.GetSession() != nil {
				sess.Observe(ctx, session.WrapMessage(pb.GetSession())) // inject into session client
			}
			return model.WrapConsumerMessage(pb), true
		})

		// Read session initialization message

		establish, ok := chanx.TryRead(sess.Establish(), 20*time.Second)
		if !ok {
			log.Errorf(ctx, "No session establish message received")
			return nil, model.WrapError(model.ErrInvalid)
		}

		// Let worker handle connect

		resp, err := c.worker.Connect(ctx, establish.ID, ch)
		if err != nil {
			log.Errorf(ctx, "Connect rejected: %v", err)
			return nil, model.WrapError(err)
		}

		joined := session.Receive(resp, out, model.NewCoordinatorSessionMessage)
		return chanx.Map(joined, model.UnwrapCoordinatorMessage), nil
	})
}
