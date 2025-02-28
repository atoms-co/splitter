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
	"go.atoms.co/splitter/pkg/service/consumer"
	"go.atoms.co/splitter/pkg/service/worker"
	"go.atoms.co/splitter/pb"
	"fmt"
)

// ConsumerService is used by clients to participate in the work distribution process.
// By using this service a client joins the work distribution process during which it receives
// assigned grants and, separately, grants assigned to all consumers.
type ConsumerService struct {
	cl       clock.Clock
	consumer consumer.Consumer
	worker   worker.Worker
}

func NewConsumerService(cl clock.Clock, c consumer.Consumer, w worker.Worker) *ConsumerService {
	return &ConsumerService{
		cl:       cl,
		consumer: c,
		worker:   w,
	}
}

func (s *ConsumerService) Join(server public_v1.ConsumerService_JoinServer) error {
	quit := iox.NewAsyncCloser()
	defer quit.Close()

	// check if connected to leader and reject otherwise
	if joined, err := s.worker.Joined(server.Context()); !joined || err != nil {
		return model.ToGRPCError(fmt.Errorf("worker not connected to Leader"))
	}

	wctx, _ := contextx.WithQuitCancel(server.Context(), quit.Closed()) // cancel context if consumer session closes

	err := grpcx.Receive(wctx, server, func(ctx context.Context, in <-chan *public_v1.JoinMessage) (<-chan *public_v1.JoinMessage, error) {
		// Read session initialization message
		establish, err := session.ReadEstablish(s.cl, in, func(m *public_v1.JoinMessage) (session.Message, bool) {
			if m.GetSession() != nil {
				return session.WrapMessage(m.GetSession()), true
			}
			return session.Message{}, false
		})
		if err != nil {
			log.Errorf(ctx, "Unabled to establish a session: %v", err)
			return nil, fmt.Errorf("%v: %w", err, model.ErrInvalid)
		}

		log.Infof(ctx, "Received establish for sid %v, (client: %v -> self: %v)", establish.ID, establish.Client, s.consumer.Self())

		consumerSession, sessionOut, established := session.NewServer(ctx, s.cl, s.consumer.Self(), establish)
		iox.WhenClosed(consumerSession, quit)

		consumerIn := chanx.MapIf(in, func(pb *public_v1.JoinMessage) (model.ConsumerMessage, bool) {
			if pb.GetSession() != nil {
				consumerSession.Observe(ctx, session.WrapMessage(pb.GetSession()))
				// Do not propagate consumer session messages to the coordinator
				return model.ConsumerMessage{}, false
			}
			return model.WrapConsumerMessage(pb.GetConsumer()), true
		})

		// Send established message to the consumer
		err = server.Send(model.UnwrapJoinMessage(model.NewJoinSessionMessage(established)))
		if err != nil {
			log.Warnf(ctx, "Send failed: %v", err)
			return nil, fmt.Errorf("send failed: %w", model.ErrInvalid)
		}

		resp, err := s.consumer.Join(ctx, consumerSession, establish.ID, consumerIn)
		if err != nil {
			return nil, err
		}

		// Inject session messages into the messages sent to the consumer
		joined := session.Receive(consumerSession, chanx.Map(resp, model.NewJoinMessage), sessionOut, model.NewJoinSessionMessage)
		return chanx.Map(joined, model.UnwrapJoinMessage), nil
	})
	return model.ToGRPCError(err)
}
