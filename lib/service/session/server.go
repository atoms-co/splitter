package session

import (
	"context"
	"atoms.co/lib-go/pkg/clock"
	"go.atoms.co/splitter/lib/service/location"
	"go.atoms.co/lib/log"
	"go.atoms.co/lib/metrics"
	"go.atoms.co/lib/clockx"
	"go.atoms.co/lib/iox"
	"fmt"
	"time"
)

const (
	// keepAliveTimeout is the default duration of a session lease
	keepAliveTimeout = 10 * time.Second
	// establishTimeout is timeout for a new but not established session.
	establishTimeout = keepAliveTimeout / 2
	// serverBufChanSize is the buffer size for session messages.
	serverBufChanSize = 20
)

var (
	serverHeartbeatLag = metrics.NewHistogram("atoms.co/libs/go/session/server_heartbeat_lag", "Heartbeat lag", metrics.JavaBucketOptions)
	numServerMessages  = metrics.NewCounter("atoms.co/libs/go/session/server_messages", "Number of messages", messageTypeKey)
)

// Server represents the server-side component of session-scoped keepalive. Can be used agnostic of transport protocol.
// Server tracks session establishment and periodically extends the session lease of a corresponding Client. The server
// is closed if the lease expires or connectivity is lost. The sentinel Closed message is not emitted into the message
// stream. Users must emit that separately to ensure it is last when merged with other messages.
type Server struct {
	iox.AsyncCloser
	cl clock.Clock

	sid         ID
	client      location.Instance
	establish   chan Establish
	established bool

	in  chan Message // never closed
	out chan<- Message
}

func NewServer(ctx context.Context, cl clock.Clock) (*Server, <-chan Message) {
	out := make(chan Message, serverBufChanSize)
	s := &Server{
		AsyncCloser: iox.NewAsyncCloser(),
		cl:          cl,
		establish:   make(chan Establish, 1),
		in:          make(chan Message, serverBufChanSize),
		out:         out,
	}
	go s.process(ctx)

	return s, out
}

// Observe observes session messages to the Server
func (s *Server) Observe(ctx context.Context, msg Message) {
	if s.IsClosed() {
		return
	}

	select {
	case s.in <- msg:
		// ok
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

func (s *Server) process(ctx context.Context) {
	defer s.Close()
	defer close(s.out)
	defer close(s.establish)

	expiration := clockx.NewTimer(s.cl, establishTimeout)
	defer expiration.Stop()

	for {
		select {
		case msg := <-s.in:
			// First message must be an Establish
			if !s.established {
				if !msg.IsEstablish() {
					log.Errorf(ctx, "Initial session message must be establish %v", msg)
					return
				}
			}

			switch {
			case msg.IsEstablish():
				establish, _ := msg.Establish()
				log.Infof(ctx, "Received establish for sid %v, client: %v", establish.ID, establish.Instance)

				s.established = true
				s.client = establish.Instance
				s.sid = establish.ID
				s.establish <- establish

				expirationTime := s.cl.Now().Add(keepAliveTimeout)
				s.send(ctx, NewEstablishedMessage(expirationTime))
				expiration.Reset(s.cl.Until(expirationTime))

			case msg.IsHeartbeat():
				now, _ := msg.Heartbeat()
				serverHeartbeatLag.Observe(ctx, s.cl.Now().Sub(now))
				expirationTime := s.cl.Now().Add(keepAliveTimeout)
				s.send(ctx, NewEstablishedMessage(expirationTime))
				expiration.Reset(s.cl.Until(expirationTime))

			case msg.IsClosed():
				return

			default:
				log.Warnf(ctx, "Received unknown session message for sid %v: %v", s.sid, msg)
			}

		case <-expiration.C:
			if s.established {
				log.Infof(ctx, "Session expired. sid: %v, client: %v", s.sid, s.client)
			} else {
				log.Infof(ctx, "Session expired before establish message.")
			}
			return

		case <-s.Closed():
			return
		case <-ctx.Done():
			return
		}
	}
}

func (s *Server) send(ctx context.Context, msg Message) {
	stuck := s.cl.NewTimer(keepAliveTimeout)
	defer stuck.Stop()

	select {
	case s.out <- msg:
		numServerMessages.Increment(ctx, 1, metrics.Tag{Key: messageTypeKey, Value: fmt.Sprintf("%v", msg.MessageType())})
	case <-stuck.C:
		log.Errorf(ctx, "Stuck server send: %v", msg)
		s.Close()
	case <-s.Closed():
	}
}
