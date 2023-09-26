package session

import (
	"context"
	"atoms.co/lib-go/pkg/clock"
	"go.atoms.co/splitter/lib/service/location"
	"go.atoms.co/lib/log"
	"go.atoms.co/lib/metrics"
	"go.atoms.co/lib/clockx"
	"go.atoms.co/lib/iox"
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
// Client establishes a lease-based session to a server with periodic heartbeats. The client is closed if the lease
// expires or connectivity is lost. The sentinel Closed message is not emitted into the message stream. Users must
// emit that separately to ensure it is last when merged with other messages.
type Client struct {
	iox.AsyncCloser
	cl clock.Clock

	sid    ID
	client location.Instance

	in  chan Message // never closed
	out chan<- Message
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
	}
	go c.process(ctx)

	return c, NewEstablishMessage(c.sid, c.client), out
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

	heartbeat := c.cl.NewTicker(heartbeatDuration)
	defer heartbeat.Stop()

	expiration := clockx.NewTimer(c.cl, pendingEstablishedTimeout)
	defer expiration.Stop()

	for !c.IsClosed() {
		select {
		case msg := <-c.in:
			switch {
			case msg.IsEstablished():
				ttl, _ := msg.Established()
				expiration.Reset(c.cl.Until(ttl))

			case msg.IsClosed():
				return

			default:
				log.Warnf(ctx, "Received unknown message for sid %v: %v", c.sid, msg)
			}

		case <-expiration.C:
			log.Infof(ctx, "Session expired. sid: %v, client: %v", c.sid, c.client)
			return

		case <-heartbeat.C:
			c.send(ctx, NewHeartbeatMessage(c.cl.Now()))

		case <-c.Closed():
			return
		case <-ctx.Done():
			return
		}
	}
}

func (c *Client) send(ctx context.Context, msg Message) {
	stuck := c.cl.NewTimer(leaseDuration)
	defer stuck.Stop()

	select {
	case c.out <- msg:
		numClientMessages.Increment(ctx, 1, messageTypeTag(msg.MessageType()))
	case <-stuck.C:
		c.Close()
	case <-c.Closed():
	}
}
