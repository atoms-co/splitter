package frontend

import (
	"context"
	"atoms.co/lib-go/pkg/clock"
	"go.atoms.co/splitter/lib/service/location"
	"go.atoms.co/splitter/lib/service/session"
	"go.atoms.co/lib/log"
	"go.atoms.co/lib/chanx"
	"go.atoms.co/lib/contextx"
	"go.atoms.co/lib/net/grpcx"
	"go.atoms.co/splitter/pkg/core"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pkg/service/worker"
	"go.atoms.co/splitter/pb/private"
	"go.atoms.co/splitter/pb"
	"google.golang.org/grpc"
	"time"
)

// ConsumerService is used by clients to participate in the work distribution process.
// By using this service a client joins the work distribution process during which it receives
// assigned grants and, separately, grants assigned to all consumers.
type ConsumerService struct {
	cl       clock.Clock
	instance location.Instance
	worker   *worker.Worker
	resolver model.Resolver[*grpc.ClientConn, model.TenantName]
}

func NewConsumerService(cl clock.Clock, loc location.Location, worker *worker.Worker, resolver core.TenantResolver) *ConsumerService {
	return &ConsumerService{
		cl:       cl,
		instance: location.NewInstance(loc),
		worker:   worker,
		resolver: resolver,
	}
}

func (s *ConsumerService) Join(server public_v1.ConsumerService_JoinServer) error {
	// Create initialize server side of a session to maintain connection to the consumer
	consumerSession, sessionOut := session.NewServer(server.Context(), s.cl)
	defer consumerSession.Close()
	wctx, _ := contextx.WithQuitCancel(server.Context(), consumerSession.Closed()) // cancel context if consumer session closes

	return grpcx.Receive(wctx, server, func(ctx context.Context, in <-chan *public_v1.ConsumerMessage) (<-chan *public_v1.ConsumerMessage, error) {
		consumerIn := chanx.MapIf(in, func(pb *public_v1.ConsumerMessage) (model.ConsumerMessage, bool) {
			if pb.GetSession() != nil {
				consumerSession.Observe(ctx, session.WrapMessage(pb.GetSession()))
				// Do not propagate consumer session messages to the coordinator
				return model.ConsumerMessage{}, false
			}
			return model.WrapConsumerMessage(pb), true
		})

		// Read session initialization message
		establish, ok := chanx.TryRead(consumerSession.Establish(), 20*time.Second)
		if !ok {
			log.Errorf(ctx, "No session establish message received")
			return nil, model.WrapError(model.ErrInvalid)
		}

		// Read registration message to determine tenant
		register, err := tryReadRegister(ctx, consumerIn)
		if err != nil {
			return nil, err
		}

		// Get a shared gRPC connection to the tenant's coordinator
		cc, err := s.resolver.Resolve(ctx, register.TenantName())
		// model.ErrNoResolution indicates a local coordinator
		if err != nil && err != model.ErrNoResolution {
			log.Debugf(ctx, "Unable to forward tenant %v: %v", register.TenantName(), err)
			return nil, err
		}

		// Setup communication with the tenant's coordinator
		var coordinatorOut <-chan model.ConsumerMessage
		if err == nil {
			coordinatorOut, err = s.forwardRemote(ctx, cc, register, consumerIn)
			if err != nil {
				return nil, err
			}
		} else {
			coordinatorOut, err = s.worker.Connect(ctx, establish.ID, register, consumerIn)
			if err != nil {
				return nil, err
			}
		}

		// Inject session messages into the messages sent to the consumer
		joined := session.Receive(consumerSession, coordinatorOut, sessionOut, model.NewConsumerSessionMessage)
		return chanx.Map(joined, model.UnwrapConsumerMessage), nil
	})
}

func (s *ConsumerService) forwardRemote(ctx context.Context, cc *grpc.ClientConn, register model.RegisterMessage, consumerIn <-chan model.ConsumerMessage) (<-chan model.ConsumerMessage, error) {
	// Create a client session with the coordinator instance
	coordinatorSession, establish, sessionOut := session.NewClient(ctx, s.cl, s.instance)
	wctx, _ := contextx.WithQuitCancel(ctx, coordinatorSession.Closed()) // cancel context if session closes

	// Use error channel to wait for stream connection. Buffered to avoid blocking goroutine on a late error
	errChan := make(chan error, 1)
	// Messages sent from the coordinator to the consumer are stored in a buffered channel
	out := make(chan model.ConsumerMessage, 100)
	go func() {
		defer coordinatorSession.Close()
		err := grpcx.Connect(wctx, internal_v1.NewCoordinatorServiceClient(cc).Connect, func(ctx context.Context, coordinatorIn <-chan *public_v1.ConsumerMessage) (<-chan *public_v1.ConsumerMessage, error) {
			// Process incoming messages by either copying to the output buffer or sending to the session
			go func() {
				for pb := range coordinatorIn {
					if pb.GetSession() != nil {
						coordinatorSession.Observe(ctx, session.WrapMessage(pb.GetSession()))
						// Do not propagate coordinator session messages to the consumer
					} else {
						out <- model.WrapConsumerMessage(pb)
					}
				}
			}()
			// Send register message first, it was read from consumer messages earlier
			consumerIn = chanx.Prepend(consumerIn, model.NewConsumerRegisterMessage(register))
			// Inject session messages into the messages sent to the coordinator
			joined := session.Connect(coordinatorSession, establish, consumerIn, sessionOut, model.NewConsumerSessionMessage)
			// Signal that connection is established
			errChan <- nil
			return chanx.Map(joined, model.UnwrapConsumerMessage), nil
		})
		if err != nil {
			log.Warnf(ctx, "Error in a stream from %v to a coordinator %v: %v", s.instance, register.TenantName(), err)
			errChan <- err
		}
	}()
	err := <-errChan
	if err != nil {
		return nil, err
	}
	return out, nil
}

func tryReadRegister(ctx context.Context, in <-chan model.ConsumerMessage) (model.RegisterMessage, error) {
	msg, ok := chanx.TryRead(in, 20*time.Second)
	if !ok {
		log.Errorf(ctx, "No register message received")
		return model.RegisterMessage{}, model.WrapError(model.ErrInvalid)
	}
	if !msg.IsRegister() {
		log.Errorf(ctx, "Expected register message, received: %v", msg)
		return model.RegisterMessage{}, model.WrapError(model.ErrInvalid)
	}
	register, _ := msg.Register()
	return register, nil
}
