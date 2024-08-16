package consumer

import (
	"context"
	"atoms.co/lib-go/pkg/clock"
	"go.atoms.co/splitter/lib/service/location"
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
	"go.atoms.co/splitter/pb/private"
	"errors"
	"fmt"
	"time"
)

// Consumer handles splitter consumer connections
type Consumer interface {
	iox.AsyncCloser

	Join(ctx context.Context, session *session.Server, sid session.ID, in <-chan model.ConsumerMessage) (<-chan model.ConsumerMessage, error)
	Self() location.Instance
}

type consumer struct {
	iox.AsyncCloser

	cl       clock.Clock
	self     location.Instance
	worker   worker.Worker
	resolver core.ServiceResolver
}

func New(cl clock.Clock, loc location.Location, w worker.Worker, resolver core.ServiceResolver) Consumer {
	c := &consumer{
		AsyncCloser: iox.NewAsyncCloser(),
		cl:          cl,
		self:        location.NewNamedInstance("consumer", loc),
		worker:      w,
		resolver:    resolver,
	}

	return c
}

func (c *consumer) Join(ctx context.Context, session *session.Server, sid session.ID, in <-chan model.ConsumerMessage) (<-chan model.ConsumerMessage, error) {
	msg, ok := chanx.TryRead(in, 20*time.Second)
	if !ok {
		log.Errorf(ctx, "No registration message received")
		return nil, model.WrapError(fmt.Errorf("no registration message received: %w", model.ErrInvalid))
	}

	clientMsg, ok := msg.ClientMessage()
	if !ok || !clientMsg.IsRegister() {
		log.Errorf(ctx, "Expected registration message, got: %v", msg)
		return nil, fmt.Errorf("expected registration message, got %v: %w", msg, model.ErrInvalid)
	}
	register, _ := clientMsg.Register()

	// Send register message first, it was read from consumer messages earlier
	in = chanx.Prepend(in, msg)

	// Get a shared gRPC connection to the service's coordinator
	cc, err := c.resolver.Resolve(ctx, register.Service())
	// model.ErrNoResolution indicates a local coordinator
	if err != nil && !errors.Is(err, model.ErrNoResolution) {
		log.Infof(ctx, "Unable to forward service %v: %v", register.Service(), err)
		return nil, model.WrapError(err)
	}

	// Setup communication with the service's coordinator
	var out <-chan model.ConsumerMessage
	if err == nil {
		out, err = c.forwardRemote(ctx, session, cc, in)
		if err != nil {
			return nil, model.WrapError(err)
		}
	} else {
		out, err = c.worker.Connect(ctx, sid, c.self, in)
		if err != nil {
			return nil, model.WrapError(err)
		}
	}

	return out, nil
}

func (c *consumer) Self() location.Instance {
	return c.self
}

func (c *consumer) forwardRemote(ctx context.Context, consumerSession *session.Server, client internal_v1.CoordinatorServiceClient, in <-chan model.ConsumerMessage) (<-chan model.ConsumerMessage, error) {
	// Create a client session with the coordinator instance
	coordinatorSession, establish, sessionOut := session.NewClient(ctx, c.cl, c.self)
	wctx, _ := contextx.WithQuitCancel(ctx, coordinatorSession.Closed()) // cancel context if session closes
	iox.WhenClosed(coordinatorSession, consumerSession)

	// Use error channel to wait for stream connection. Buffered to avoid blocking goroutine on a late error
	errChan := make(chan error, 1)
	// Messages sent from the coordinator to the consumer are stored in a buffered channel
	out := make(chan model.ConsumerMessage, 100)
	go func() {
		defer coordinatorSession.Close()
		err := grpcx.Connect(wctx, client.Connect, func(ctx context.Context, coordinatorIn <-chan *internal_v1.ConnectMessage) (<-chan *internal_v1.ConnectMessage, error) {
			// Process incoming messages by either copying to the output buffer or sending to the session
			go func() {
				for pb := range coordinatorIn {
					if pb.GetSession() != nil {
						coordinatorSession.Observe(ctx, session.WrapMessage(pb.GetSession()))
						// Do not propagate coordinator session messages to the consumer
					} else {
						out <- model.WrapConsumerMessage(pb.GetConsumer())
					}
				}
			}()
			// Signal that connection is established
			errChan <- nil

			connect := chanx.Map(in, coordinator.NewConnectConsumerMessage)
			joined := session.Connect(coordinatorSession, establish, connect, sessionOut, coordinator.NewConnectSessionMessage)
			return chanx.Map(joined, coordinator.UnwrapConnectMessage), nil
		})
		if err != nil {
			log.Warnf(ctx, "Error in a stream from %v to a coordinator: %v", c.self, err)
			errChan <- err
		}
	}()
	err := <-errChan
	if err != nil {
		return nil, err
	}
	return out, nil
}
