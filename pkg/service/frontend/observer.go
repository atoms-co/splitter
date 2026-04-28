package frontend

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.atoms.co/splitter/lib/service/session"
	"go.atoms.co/lib/log"
	"go.atoms.co/lib/chanx"
	"go.atoms.co/lib/contextx"
	"go.atoms.co/lib/net/grpcx"
	"go.atoms.co/iox"
	"go.atoms.co/splitter/pkg/core"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pkg/service/worker"
	splitterprivatepb "go.atoms.co/splitter/pb/private"
)

type ObserverService struct {
	worker   worker.Worker
	resolver core.ServiceResolver
}

func NewObserverService(w worker.Worker, resolver core.ServiceResolver) *ObserverService {
	return &ObserverService{
		worker:   w,
		resolver: resolver,
	}
}

func (o *ObserverService) Observe(server splitterprivatepb.ObserverService_ObserveServer) error {
	quit := iox.NewAsyncCloser()
	defer quit.Close()

	wctx, _ := contextx.WithQuitCancel(server.Context(), quit.Closed())

	err := grpcx.Receive(wctx, server, func(ctx context.Context, in <-chan *splitterprivatepb.ObserverClientMessage) (<-chan *splitterprivatepb.ObserverServerMessage, error) {
		establish, err := session.ReadEstablish(in, func(m *splitterprivatepb.ObserverClientMessage) (session.Message, bool) {
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

		sess, sessionOut, established := session.NewServer(ctx, o.worker.Self(), establish)
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

		msg, ok := chanx.TryRead(observerIn, 20*time.Second)
		if !ok {
			log.Errorf(ctx, "No registration message received")
			return nil, fmt.Errorf("no registration message received: %w", model.ErrInvalid)
		}

		if !msg.IsRegister() {
			log.Errorf(ctx, "Expected registration message, got: %v", msg)
			return nil, fmt.Errorf("expected registration message: %w", model.ErrInvalid)
		}
		register, _ := msg.Register()
		service, err := register.Service()
		if err != nil {
			log.Errorf(ctx, "Invalid service name: %v", err)
			return nil, fmt.Errorf("invalid service name: %w", model.ErrInvalid)
		}
		log.Infof(ctx, "Received observer registration for service %v", service)

		observerIn = chanx.Prepend(observerIn, msg)

		cc, err := o.resolver.Resolve(ctx, service)
		if err != nil && !errors.Is(err, model.ErrNoResolution) {
			log.Infof(ctx, "Unable to forward service %v: %v", service, err)
			return nil, err
		}

		var out <-chan core.ObserverServerMessage
		if err == nil {
			// Remote coordinator - forward the request
			out, err = o.forwardRemote(ctx, sess, service, cc, observerIn)
			if err != nil {
				return nil, model.FromGRPCError(err)
			}
		} else {
			// Local coordinator - handle locally
			out, err = o.worker.Observe(ctx, establish.ID, establish.Client, observerIn)
			if err != nil {
				return nil, err
			}
		}

		joined := session.Receive(sess, chanx.Map(out, core.UnwrapObserverServerMessage), sessionOut, func(msg session.Message) *splitterprivatepb.ObserverServerMessage {
			return core.UnwrapObserverServerMessage(core.NewObserverServerMessage(msg))
		})
		return joined, nil
	})
	return model.ToGRPCError(err)
}

func (o *ObserverService) forwardRemote(ctx context.Context, observerSession *session.Server, service model.QualifiedServiceName, client splitterprivatepb.CoordinatorServiceClient, in <-chan core.ObserverClientMessage) (<-chan core.ObserverServerMessage, error) {
	coordinatorSession, establish, sessionOut := session.NewClient(ctx, o.worker.Self())
	wctx, _ := contextx.WithQuitCancel(ctx, coordinatorSession.Closed())
	iox.WhenClosed(coordinatorSession, observerSession)

	errChan := make(chan error, 1)
	out := make(chan core.ObserverServerMessage, 100)

	go func() {
		defer coordinatorSession.Close()
		defer close(out)
		err := grpcx.Connect(wctx, client.Observe, func(ctx context.Context, coordinatorIn <-chan *splitterprivatepb.ObserverServerMessage) (<-chan *splitterprivatepb.ObserverClientMessage, error) {
			go func() {
				for pb := range coordinatorIn {
					if pb.GetSession() != nil {
						coordinatorSession.Observe(ctx, session.WrapMessage(pb.GetSession()))
					} else {
						out <- core.WrapObserverServerMessage(pb)
					}
				}
			}()
			errChan <- nil

			joined := session.Connect(coordinatorSession, establish, chanx.Map(in, core.UnwrapObserverClientMessage), sessionOut, func(m session.Message) *splitterprivatepb.ObserverClientMessage {
				return core.UnwrapObserverClientMessage(core.NewObserverClientMessage(m))
			})
			return joined, nil
		})
		if err != nil {
			log.Warnf(ctx, "Error in observer stream forwarding to remote coordinator for service %v: %v", service, err)
			errChan <- err
		}
	}()

	err := <-errChan
	if err != nil {
		return nil, err
	}
	return out, nil
}
