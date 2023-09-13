package session

import (
	"context"
	"atoms.co/lib-go/pkg/clock"
	"go.atoms.co/splitter/lib/service/location"
	"go.atoms.co/lib/log"
	"go.atoms.co/lib/metrics"
	"go.atoms.co/lib/clockx"
	"go.atoms.co/lib/iox"
	"time"
)

const (
	// heartbeatDuration is a duration of the heartbeat interval.
	heartbeatDuration = leaseDuration / 3
	// clientBufChanSize is the buffer size for session messages.
	clientBufChanSize = 20
)

var (
	numClientMessages = metrics.NewCounter("atoms.co/libs/go/session/client_messages", "Number of messages", messageTypeKey)
)

// Client represents the client-side component of session-scoped keepalive. Can be used agnostic of transport protocol.
// Client establishes a lease-based session to a server with periodic heartbeats.
type Client struct {
	iox.AsyncCloser
	cl clock.Clock

	sid    ID
	client location.Instance

	in       chan Message
	out      chan Message
	draining iox.AsyncCloser
}

func NewClient(ctx context.Context, cl clock.Clock, client location.Instance) (*Client, Message, <-chan Message) {
	out := make(chan Message, clientBufChanSize)
	c := &Client{
		AsyncCloser: iox.NewAsyncCloser(),
		cl:          cl,
		sid:         NewID(),
		client:      client,
		in:          make(chan Message, clientBufChanSize),
		out:         out,
		draining:    iox.NewAsyncCloser(),
	}
	go c.process(ctx)

	return c, NewEstablishMessage(c.sid, c.client), out
}

// Observe observes session messages to the Client
func (c *Client) Observe(ctx context.Context, msg Message) {
	if c.IsClosed() || c.draining.IsClosed() {
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

// Drain drains the Client by sending a Close message to the Server
func (c *Client) Drain(timeout time.Duration) {
	c.draining.Close()
	c.cl.AfterFunc(timeout, c.Close)
}

func (c *Client) process(ctx context.Context) {
	defer c.Close()
	defer close(c.out)

	heartbeat := c.cl.NewTicker(heartbeatDuration)
	defer heartbeat.Stop()

	expiration := clockx.NewTimer(c.cl, pendingEstablishedTimeout)
	defer expiration.Stop()

	for {
		select {
		case msg := <-c.in:
			switch {
			case msg.IsEstablished():
				ttl, _ := msg.Established()
				expiration.Reset(c.cl.Until(ttl))
			case msg.IsClosed():
				log.Infof(ctx, "Session closed. sid: %v, client: %v", c.sid, c.client)
				return
			default:
				log.Warnf(ctx, "Received unknown message for sid %v: %v", c.sid, msg)
			}
		case <-expiration.C:
			log.Infof(ctx, "Session expired. sid: %v, client: %v", c.sid, c.client)
			return
		case <-heartbeat.C:
			c.send(ctx, NewHeartbeatMessage(c.cl.Now()))
		case <-c.draining.Closed():
			c.send(ctx, NewCloseMessage())
			return
		case <-c.Closed():
			return
		case <-ctx.Done():
			return
		}
	}
}

func (c *Client) send(ctx context.Context, msg Message) {
	select {
	case c.out <- msg:
		numClientMessages.Increment(ctx, 1, messageTypeTag(msg.MessageType()))
	case <-c.Closed():
	}
}
