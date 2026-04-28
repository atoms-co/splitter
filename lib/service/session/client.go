package session

import (
	"context"
	"time"

	"go.atoms.co/splitter/lib/service/location"
	"go.atoms.co/lib/log"
	"go.atoms.co/lib/metrics"
	"go.atoms.co/iox"
)

const (
	// defaultHeartbeatDuration is the default duration of the heartbeat interval.
	defaultHeartbeatDuration = 5 * time.Second
	// clientBufChanSize is the buffer size for session messages.
	clientBufChanSize = 20
)

var (
	numClientMessages = metrics.NewCounter("atoms.co/libs/go/session/client_messages", "Number of messages", messageTypeKey)
)

// Client represents the client-side component of session-scoped keepalive. Can be used agnostic of transport protocol.
// Client establishes a lease-based session to a server with periodic heartbeats. The client is closed if the lease
// expires or connectivity is lost. The sentinel Closed message is not emitted into the message stream. Users must
// emit that separately to ensure it is last when merged with other messages.
type Client struct {
	iox.AsyncCloser

	sid    ID
	self   location.Instance
	server location.Instance

	in  chan Message // never closed
	out chan<- Message

	keepAliveTimeout  time.Duration
	heartbeatDuration time.Duration
}

type ClientOption func(*Client)

// WithClientKeepAliveTimeout sets the keep-alive timeout sessions. The client will close the session if a heartbeat
// is stuck for greater than this duration or if the establish message is not acked within this duration.
func WithClientKeepAliveTimeout(timeout time.Duration) ClientOption {
	return func(c *Client) {
		c.keepAliveTimeout = timeout
	}
}

// WithClientHeartbeatDuration sets the heartbeat duration for established sessions. The server should receive a
// heartbeat with its keep alive duration or it will close the session. Typically, the heartbeat duration should be half
// the server keep alive timeout.
func WithClientHeartbeatDuration(duration time.Duration) ClientOption {
	return func(c *Client) {
		c.heartbeatDuration = duration
	}
}

func NewClient(ctx context.Context, self location.Instance, options ...ClientOption) (*Client, Message, <-chan Message) {
	out := make(chan Message, clientBufChanSize)
	c := &Client{
		AsyncCloser:       iox.NewAsyncCloser(),
		sid:               NewID(),
		self:              self,
		in:                make(chan Message, clientBufChanSize),
		out:               out,
		keepAliveTimeout:  defaultKeepAliveTimeout,
		heartbeatDuration: defaultHeartbeatDuration,
	}

	for _, opt := range options {
		opt(c)
	}

	go c.process(ctx)

	return c, NewEstablishMessage(c.sid, c.self), out
}

// Observe observes session messages to the Client
func (c *Client) Observe(ctx context.Context, msg Message) {
	if c.IsClosed() {
		return
	}

	select {
	case c.in <- msg:
	case <-ctx.Done():
		return
	case <-c.Closed():
		return
	}
}

func (c *Client) process(ctx context.Context) {
	defer c.Close()
	defer close(c.out)

	heartbeat := time.NewTicker(c.heartbeatDuration)
	defer heartbeat.Stop()

	expiration := time.NewTimer(c.keepAliveTimeout)
	defer expiration.Stop()

	for !c.IsClosed() {
		select {
		case msg := <-c.in:


			switch {
			case msg.IsEstablished():
				established, _ := msg.Established()
				c.server = established.Server
				expiration.Reset(time.Until(established.Ttl))

			case msg.IsHeartbeatAck():
				ttl, _ := msg.HeartbeatAck()
				expiration.Reset(time.Until(ttl))

			case msg.IsClosed():
				return

			default:
				log.Warnf(ctx, "Received unknown message for sid %v, (self: %v -> server: %v): %v", c.sid, c.self, c.server, msg)
			}

		case <-expiration.C:
			log.Infof(ctx, "Session expired. sid: %v, (self: %v -> server: %v)", c.sid, c.self, c.server)
			return

		case <-heartbeat.C:
			c.send(ctx, NewHeartbeatMessage(time.Now()))

		case <-c.Closed():
			return
		case <-ctx.Done():
			return
		}
	}
}

func (c *Client) send(ctx context.Context, msg Message) {
	stuck := time.NewTimer(c.keepAliveTimeout)
	defer stuck.Stop()

	select {
	case c.out <- msg:
		numClientMessages.Increment(ctx, 1, messageTypeTag(msg.MessageType()))
	case <-stuck.C:
		log.Errorf(ctx, "Stuck client send: %v", msg)
		c.Close()
	case <-c.Closed():
	}
}
