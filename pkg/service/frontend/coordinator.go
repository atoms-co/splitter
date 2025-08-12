package frontend

import (
	"context"
	"fmt"

	"atoms.co/lib-go/pkg/clock"
	"go.atoms.co/splitter/lib/service/session"
	"go.atoms.co/lib/log"
	"go.atoms.co/lib/chanx"
	"go.atoms.co/lib/contextx"
	"go.atoms.co/lib/net/grpcx"
	"go.atoms.co/lib/iox"
	"go.atoms.co/splitter/pkg/core"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pkg/service/coordinator"
	"go.atoms.co/splitter/pkg/service/worker"
	splitterprivatepb "go.atoms.co/splitter/pb/private"
)

// CoordinatorService is a grpc frontend for the internal coordinator api.
type CoordinatorService struct {
	cl     clock.Clock
	worker worker.Worker
}

func NewCoordinatorService(cl clock.Clock, w worker.Worker) *CoordinatorService {
	c := CoordinatorService{
		cl:     cl,
		worker: w,
	}
	return &c
}

func (c *CoordinatorService) Connect(server splitterprivatepb.CoordinatorService_ConnectServer) error {
	quit := iox.NewAsyncCloser()
	defer quit.Close()

	wctx, _ := contextx.WithQuitCancel(server.Context(), quit.Closed()) // cancel context if session server closes

	err := grpcx.Receive(wctx, server, func(ctx context.Context, in <-chan *splitterprivatepb.ConnectMessage) (<-chan *splitterprivatepb.ConnectMessage, error) {
		// Read session initialization message
		establish, err := session.ReadEstablish(c.cl, in, func(m *splitterprivatepb.ConnectMessage) (session.Message, bool) {
			if m.GetSession() != nil {
				return session.WrapMessage(m.GetSession()), true
			}
			return session.Message{}, false
		})
		if err != nil {
			log.Errorf(ctx, "Unabled to establish a session: %v", err)
			return nil, fmt.Errorf("%v: %w", err, model.ErrInvalid)
		}

		log.Infof(ctx, "Received establish for sid %v, (client: %v -> self: %v)", establish.ID, establish.Client, c.worker.Self())

		sess, out, established := session.NewServer(ctx, c.cl, c.worker.Self(), establish)
		iox.WhenClosed(sess, quit)

		ch := chanx.MapIf(in, func(pb *splitterprivatepb.ConnectMessage) (model.ConsumerMessage, bool) {
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
			return nil, fmt.Errorf("send failed: %w", model.ErrInvalid)
		}

		// Let worker handle connect
		resp, err := c.worker.Connect(ctx, establish.ID, establish.Client, ch)
		if err != nil {
			if !model.IsOwnershipError(err) {
				log.Warnf(ctx, "Internal: connect from %v rejected: %v", establish, err)
			}
			return nil, err
		}

		joined := session.Receive(sess, chanx.Map(resp, coordinator.NewConnectConsumerMessage), out, coordinator.NewConnectSessionMessage)
		return chanx.Map(joined, coordinator.UnwrapConnectMessage), nil
	})
	return model.ToGRPCError(err)
}

func (c *CoordinatorService) Handle(ctx context.Context, request *splitterprivatepb.CoordinatorHandleRequest) (*splitterprivatepb.CoordinatorHandleResponse, error) {
	resp, err := c.worker.Handle(ctx, coordinator.HandleRequest{Proto: request})
	return resp, model.ToGRPCError(err)
}

func (c *CoordinatorService) Observe(server splitterprivatepb.CoordinatorService_ObserveServer) error {
	quit := iox.NewAsyncCloser()
	defer quit.Close()

	wctx, _ := contextx.WithQuitCancel(server.Context(), quit.Closed())

	err := grpcx.Receive(wctx, server, func(ctx context.Context, in <-chan *splitterprivatepb.ObserverClientMessage) (<-chan *splitterprivatepb.ObserverServerMessage, error) {
		establish, err := session.ReadEstablish(c.cl, in, func(m *splitterprivatepb.ObserverClientMessage) (session.Message, bool) {
			wrapped := core.WrapObserverClientMessage(m)
			if sess, ok := wrapped.Session(); ok {
				return sess, true
			}
			return session.Message{}, false
		})
		if err != nil {
			log.Errorf(ctx, "Unable to establish observer session: %v", err)
			return nil, fmt.Errorf("%v: %w", err, model.ErrInvalid)
		}

		log.Infof(ctx, "Received establish for sid %v, (client: %v -> self: %v)", establish.ID, establish.Client, c.worker.Self())

		sess, sessionOut, established := session.NewServer(ctx, c.cl, c.worker.Self(), establish)
		iox.WhenClosed(sess, quit)

		observerIn := chanx.MapIf(in, func(pb *splitterprivatepb.ObserverClientMessage) (core.ObserverClientMessage, bool) {
			wrapped := core.WrapObserverClientMessage(pb)
			if s, ok := wrapped.Session(); ok {
				sess.Observe(ctx, s)
				return core.ObserverClientMessage{}, false
			}
			return wrapped, true
		})

		err = server.Send(core.UnwrapObserverServerMessage(core.NewObserverServerMessage(established)))
		if err != nil {
			log.Warnf(ctx, "Send failed: %v", err)
			return nil, fmt.Errorf("send failed: %w", model.ErrInvalid)
		}

		resp, err := c.worker.Observe(ctx, establish.ID, establish.Client, observerIn)
		if err != nil {
			if !model.IsOwnershipError(err) {
				log.Warnf(ctx, "Internal: observe from %v rejected: %v", establish, err)
			}
			return nil, err
		}

		joined := session.Receive(sess, chanx.Map(resp, core.UnwrapObserverServerMessage), sessionOut, func(msg session.Message) *splitterprivatepb.ObserverServerMessage {
			return core.UnwrapObserverServerMessage(core.NewObserverServerMessage(msg))
		})
		return joined, nil
	})
	return model.ToGRPCError(err)
}
