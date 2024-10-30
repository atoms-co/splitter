package sessionx

import (
	"context"
	"atoms.co/lib-go/pkg/clock"
	"go.atoms.co/splitter/lib/service/session"
	"go.atoms.co/lib/log"
	"go.atoms.co/lib/iox"
	"go.atoms.co/splitter/pkg/model"
	"fmt"
	"sync/atomic"
	"time"
)

// Connection represents an async session-enabled connection. Useful for muxing multiple session streams into one.
type Connection[T any] interface {
	iox.AsyncCloser

	Send(context.Context, T) bool
	Sid() session.ID
	Instance() model.Instance
	Disconnect()
}

type connection[T any] struct {
	iox.AsyncCloser

	cl       clock.Clock
	sid      session.ID
	instance model.Instance
	out      chan<- T

	closed atomic.Bool
}

type Message[T any] struct {
	Sid      session.ID
	Instance model.Instance
	Msg      T
}

func NewConnection[T any](cl clock.Clock, sid session.ID, instance model.Instance, closer iox.AsyncCloser, in <-chan T, messages chan<- *Message[T]) (Connection[T], <-chan T) {
	out := make(chan T, 100)
	con := &connection[T]{
		AsyncCloser: iox.WithQuit(closer.Closed(), iox.NewAsyncCloser()),
		sid:         sid,
		instance:    instance,
		cl:          cl,
		out:         out,
	}
	go con.forward(in, messages)
	return con, out
}

func (c *connection[T]) Send(ctx context.Context, msg T) bool {
	if c.IsClosed() {
		return false
	}

	timer := c.cl.NewTimer(5 * time.Second)
	defer timer.Stop()

	select {
	case c.out <- msg:
		return true
	case <-c.Closed():
		return false
	case <-timer.C:
		log.Errorf(ctx, "Consumer connection %v is stuck. Closing", c)
		c.Close()
		return false
	}
}

func (c *connection[T]) Sid() session.ID {
	return c.sid
}

func (c *connection[T]) Instance() model.Instance {
	return c.instance
}

func (c *connection[T]) Disconnect() {
	c.Close()
	if c.closed.CompareAndSwap(false, true) {
		close(c.out)
	}
}

func (c *connection[T]) forward(in <-chan T, messages chan<- *Message[T]) {
	defer c.Close()

	for {
		select {
		case msg, ok := <-in:
			if !ok {
				return
			}
			select {
			case messages <- &Message[T]{Sid: c.sid, Instance: c.instance, Msg: msg}:
			case <-c.Closed():
				return
			}
		case <-c.Closed():
			return
		}
	}
}

func (c *connection[T]) String() string {
	return fmt.Sprintf("%v[instance=%v]", c.sid, c.instance)
}
