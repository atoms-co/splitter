package session

import (
	"context"
	"atoms.co/lib-go/pkg/clock"
	"go.atoms.co/lib/log"
	"go.atoms.co/lib/metrics"
	"go.atoms.co/lib/chanx"
	"go.atoms.co/lib/iox"
	"fmt"
	"time"
)

const (
	// leaseDuration is the default duration of a session lease
	leaseDuration = 30 * time.Second
	// pendingEstablishedTimeout is timeout for a new but not established session.
	pendingEstablishedTimeout = leaseDuration / 2
	// serverBufChanSize is the buffer size for session messages.
	serverBufChanSize = 20
)

var (
	numServerMessages = metrics.NewCounter("atoms.co/libs/go/session/server_messages", "Number of messages", sessionIDKey, messageTypeKey)
)

// Server represents the server-side component of session-scoped keepalive. Can be used agnostic of transport protocol.
// Server tracks session establishment and periodically extends the session lease of a corresponding Client.
type Server struct {
	iox.AsyncCloser
	cl clock.Clock

	sid         ID
	client      ClientDefinition
	establish   chan Establish
	established bool

	in       chan Message
	out      chan Message
	draining iox.AsyncCloser
}

func NewServer(ctx context.Context, cl clock.Clock) (*Server, <-chan Message) {
	out := make(chan Message, serverBufChanSize)
	s := &Server{
		AsyncCloser: iox.NewAsyncDrainer(),
		cl:          cl,
		establish:   make(chan Establish, 1),
		in:          make(chan Message, serverBufChanSize),
		out:         out,
		draining:    iox.NewAsyncCloser(),
	}
	go s.process(ctx)

	return s, out
}

// Observe observes session messages to the Server
func (s *Server) Observe(ctx context.Context, msg Message) {
	if s.IsClosed() || s.draining.IsClosed() {
		return
	}

	select {
	case s.in <- msg:
	case <-ctx.Done():
		return
	case <-s.Closed():
		return
	}
}

// Establish returns a channel for a user to wait for the singleton Establish message from the Client
func (s *Server) Establish() chan Establish {
	return s.establish
}

// Drain drains the Server by sending a Closed message to the Client
func (s *Server) Drain(timeout time.Duration) {
	s.draining.Close()
	s.cl.AfterFunc(timeout, s.Close)
}

func (s *Server) process(ctx context.Context) {
	defer s.Close()
	defer close(s.out)
	defer close(s.establish)

	expiration := s.cl.NewTicker(pendingEstablishedTimeout)
	defer expiration.Stop()

	for {
		select {
		case msg := <-s.in:
			// First message must be establish
			if !s.established {
				if !msg.IsEstablish() {
					log.Infof(ctx, "Received message before establish %v", msg)
					return
				}
			}

			switch {
			case msg.IsEstablish():
				establish, _ := msg.Establish()
				log.Infof(ctx, "Received establish for sid %v, client: %v", establish.ID, establish.Definition)

				// Establish session
				s.established = true
				s.client = establish.Definition
				s.sid = establish.ID
				s.establish <- establish

				expirationTime := s.cl.Now().Add(leaseDuration)
				s.send(ctx, NewEstablishedMessage(expirationTime))
				// reset expiration
				chanx.Drain(expiration.C)
				expiration.Reset(s.cl.Until(expirationTime))

			case msg.IsHeartbeat():
				now, _ := msg.Heartbeat()
				expirationTime := now.Add(leaseDuration)

				s.send(ctx, NewEstablishedMessage(expirationTime))
				// reset expiration
				chanx.Drain(expiration.C)
				expiration.Reset(s.cl.Until(expirationTime))
			case msg.IsClose():
				log.Infof(ctx, "Session closed")
				return
			default:
				log.Warnf(ctx, "Received unknown message for sid %v: %v", s.sid, msg)
			}
		case <-expiration.C:
			if s.established {
				log.Infof(ctx, "Session expired. sid: %v, client: %v", s.sid, s.client)
			} else {
				log.Infof(ctx, "Session expired before establish message.")
			}
			return
		case <-s.draining.Closed():
			s.send(ctx, NewClosedMessage())
			return
		case <-s.Closed():
			return
		case <-ctx.Done():
			return
		}
	}
}

func (s *Server) send(ctx context.Context, msg Message) {
	select {
	case s.out <- msg:
		numServerMessages.Increment(
			ctx,
			1,
			metrics.Tag{Key: sessionIDKey, Value: fmt.Sprintf("%v", s.sid)},
			metrics.Tag{Key: messageTypeKey, Value: fmt.Sprintf("%v", msg.MessageType())},
		)
	case <-s.Closed():
	}
}
