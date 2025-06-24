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
	"go.atoms.co/splitter/pkg/service/worker"
	splitterprivatepb "go.atoms.co/splitter/pb/private"
)

type ObserverService struct {
	cl     clock.Clock
	worker worker.Worker
}

func NewObserverService(cl clock.Clock, w worker.Worker) *ObserverService {
	return &ObserverService{
		cl:     cl,
		worker: w,
	}
}

func (o *ObserverService) Observe(server splitterprivatepb.ObserverService_ObserveServer) error {
	quit := iox.NewAsyncCloser()
	defer quit.Close()

	wctx, _ := contextx.WithQuitCancel(server.Context(), quit.Closed())

	err := grpcx.Receive(wctx, server, func(ctx context.Context, in <-chan *splitterprivatepb.ObserverClientMessage) (<-chan *splitterprivatepb.ObserverServerMessage, error) {
		establish, err := session.ReadEstablish(o.cl, in, func(m *splitterprivatepb.ObserverClientMessage) (session.Message, bool) {
			wrapped := core.WrapObserverClientMessage(m)
			if s, ok := wrapped.Session(); ok {
				return s, true
			}
			return session.Message{}, false
		})
		if err != nil {
			log.Errorf(ctx, "Unable to establish observer session: %v", err)
			return nil, fmt.Errorf("%v: %w", err, model.ErrInvalid)
		}

		log.Infof(ctx, "Received observer establish for sid %v, (client: %v -> self: %v)", establish.ID, establish.Client, o.worker.Self())

		sess, sessionOut, established := session.NewServer(ctx, o.cl, o.worker.Self(), establish)
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

		resp, err := o.worker.Observe(ctx, establish.ID, establish.Client, observerIn)
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
