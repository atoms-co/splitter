package frontend

import (
	"context"
	"atoms.co/lib-go/pkg/clock"
	"go.atoms.co/splitter/lib/service/session"
	"go.atoms.co/lib/log"
	"go.atoms.co/lib/chanx"
	"go.atoms.co/lib/contextx"
	"go.atoms.co/lib/net/grpcx"
	"go.atoms.co/lib/iox"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pkg/service/coordinator"
	"go.atoms.co/splitter/pkg/service/worker"
	"go.atoms.co/splitter/pb/private"
	"fmt"
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
	quit := iox.NewAsyncCloser()
	defer quit.Close()

	wctx, _ := contextx.WithQuitCancel(server.Context(), quit.Closed()) // cancel context if session server closes

	return grpcx.Receive(wctx, server, func(ctx context.Context, in <-chan *internal_v1.ConnectMessage) (<-chan *internal_v1.ConnectMessage, error) {
		// Read session initialization message
		establish, err := session.ReadEstablish(in, func(m *internal_v1.ConnectMessage) (session.Message, bool) {
			if m.GetSession() != nil {
				return session.WrapMessage(m.GetSession()), true
			}
			return session.Message{}, false
		})
		if err != nil {
			log.Errorf(ctx, "Unabled to establish a session: %v", err)
			return nil, model.WrapError(fmt.Errorf("%v: %w", err, model.ErrInvalid))
		}

		log.Infof(ctx, "Received establish for sid %v, (client: %v -> self: %v)", establish.ID, establish.Client, c.worker.Self().Instance())

		sess, out, established := session.NewServer(ctx, c.cl, c.worker.Self().Instance(), establish)
		iox.WhenClosed(sess, quit)

		ch := chanx.MapIf(in, func(pb *internal_v1.ConnectMessage) (model.ConsumerMessage, bool) {
			if pb.GetSession() != nil {
				sess.Observe(ctx, session.WrapMessage(pb.GetSession())) // inject into session client
				return model.ConsumerMessage{}, false
			}
			return model.WrapConsumerMessage(pb.GetConsumer()), true
		})

		// Send established message to the consumer
		err = server.Send(coordinator.UnwrapConnectMessage(coordinator.NewConnectSessionMessage(established)))
		if err != nil {
			log.Warnf(ctx, "Send failed: %v", err)
			return nil, model.WrapError(fmt.Errorf("send failed: %w", model.ErrInvalid))
		}

		// Let worker handle connect
		resp, err := c.worker.Connect(ctx, establish.ID, ch)
		if err != nil {
			if !model.IsOwnershipError(err) {
				log.Warnf(ctx, "Internal: connect from %v rejected: %v", establish, err)
			}
			return nil, model.WrapError(err)
		}

		joined := session.Receive(sess, chanx.Map(resp, coordinator.NewConnectConsumerMessage), out, coordinator.NewConnectSessionMessage)
		return chanx.Map(joined, coordinator.UnwrapConnectMessage), nil
	})
}

func (c *CoordinatorService) Handle(ctx context.Context, request *internal_v1.CoordinatorHandleRequest) (*internal_v1.CoordinatorHandleResponse, error) {
	resp, err := c.worker.Handle(ctx, coordinator.HandleRequest{Proto: request})
	return resp, model.WrapError(err)
}
