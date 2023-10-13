package sessionx

import (
	"context"
	"atoms.co/lib-go/pkg/clock"
	"go.atoms.co/splitter/lib/service/session"
	"go.atoms.co/lib/log"
	"go.atoms.co/lib/iox"
	"fmt"
	"time"
)

// Connection represents an async session-enabled connection. Useful for muxing multiple session streams into one.
type Connection[T any] interface {
	iox.AsyncCloser

	Send(context.Context, T) bool
}

type connection[T any] struct {
	iox.AsyncCloser

	cl  clock.Clock
	sid session.ID
	out chan<- T
}

type Message[T any] struct {
	Sid session.ID
	Msg T
}

func NewConnection[T any](cl clock.Clock, sid session.ID, closer iox.AsyncCloser, in <-chan T, messages chan<- *Message[T]) (Connection[T], <-chan T) {
	out := make(chan T, 100)
	con := &connection[T]{
		AsyncCloser: iox.WithQuit(closer.Closed(), iox.NewAsyncCloser()),
		sid:         sid,
		cl:          cl,
		out:         out,
	}
	go con.forward(in, messages)
	return con, out
}

func (c connection[T]) Send(ctx context.Context, msg T) bool {
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

func (c connection[T]) forward(in <-chan T, messages chan<- *Message[T]) {
	defer c.Close()
	for {
		select {
		case msg, ok := <-in:
			if !ok {
				return
			}
			select {
			case messages <- &Message[T]{Sid: c.sid, Msg: msg}:
			case <-c.Closed():
				return
			}
		case <-c.Closed():
			return
		}
	}
}

func (c connection[T]) String() string {
	return fmt.Sprintf("sid: %v", c.sid)
}
