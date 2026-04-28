package session

import (
	"context"
	"fmt"
	"time"

	"go.atoms.co/splitter/lib/service/location"
	"go.atoms.co/lib/log"
	"go.atoms.co/lib/metrics"
	"go.atoms.co/iox"
)

const (
	// defaultKeepAliveTimeout is the default duration of a session lease
	defaultKeepAliveTimeout = 10 * time.Second
	// defaultEstablishTimeout is timeout for a new but not established session.
	defaultEstablishTimeout = defaultKeepAliveTimeout / 2
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

	sid    ID
	self   location.Instance
	client location.Instance

	in  chan Message // never closed
	out chan<- Message

	keepAliveTimeout time.Duration
	establishTimeout time.Duration
}

type ServerOption func(*Server)

// WithServerKeepAliveTimeout sets the keep-alive timeout for established sessions. The server will close the session
// if no messages are received within this timeout. The server also uses this value to set the TTL on ack messages.
func WithServerKeepAliveTimeout(timeout time.Duration) ServerOption {
	return func(s *Server) {
		s.keepAliveTimeout = timeout
	}
}

// NewServer creates and initializes a new server-side session. The session uses the given Establish
// for initialization and returns a corresponding Established message which must be sent to the client.
// Returns the Server, a channel with outgoing messages and the Established message to be sent to the client.
func NewServer(ctx context.Context, self location.Instance, establish Establish, options ...ServerOption) (*Server, <-chan Message, Message) {
	out := make(chan Message, serverBufChanSize)
	s := &Server{
		AsyncCloser:      iox.NewAsyncCloser(),
		sid:              establish.ID,
		self:             self,
		client:           establish.Client,
		in:               make(chan Message, serverBufChanSize),
		out:              out,
		keepAliveTimeout: defaultKeepAliveTimeout,
	}

	for _, opt := range options {
		opt(s)
	}

	expiration := time.Now().Add(s.keepAliveTimeout)
	established := NewEstablishedMessage(expiration, s.self)

	go s.process(ctx, expiration)

	return s, out, established
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

func (s *Server) process(ctx context.Context, ttl time.Time) {
	defer s.Close()
	defer close(s.out)

	expiration := time.NewTimer(time.Until(ttl))
	defer expiration.Stop()

	for {
		select {
		case msg := <-s.in:
			switch {
			case msg.IsEstablish():
				log.Errorf(ctx, "Received an establish message after establishing a session: %v. Terminating.", msg)
				return

			case msg.IsHeartbeat():
				now, _ := msg.Heartbeat()
				serverHeartbeatLag.Observe(ctx, time.Now().Sub(now))
				expirationTime := time.Now().Add(s.keepAliveTimeout)
				s.send(ctx, NewHeartbeakAckMessage(expirationTime))
				expiration.Reset(time.Until(expirationTime))

			case msg.IsClosed():
				return

			default:
				log.Warnf(ctx, "Received unknown session message for sid %v, (client: %v -> self: %v): %v", s.sid, s.client, s.self, msg)
			}

		case <-expiration.C:
			log.Infof(ctx, "Session expired. sid: %v, (client: %v -> self: %v)", s.sid, s.client, s.self)
			return

		case <-s.Closed():
			return
		case <-ctx.Done():
			return
		}
	}
}

func (s *Server) send(ctx context.Context, msg Message) {
	stuck := time.NewTimer(s.keepAliveTimeout)
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
