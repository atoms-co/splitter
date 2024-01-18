package session_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"go.atoms.co/splitter/lib/service/location"
	"go.atoms.co/splitter/lib/service/session"
	"go.atoms.co/lib/testing/assertx"
	"go.atoms.co/lib/testing/mockclock"
)

func TestConnect(t *testing.T) {
	ctx := context.Background()
	cl := mockclock.NewUnsynchronized()
	cl.Set(time.Now())

	loc := location.NewInstance(location.New("us-west2", "unknown"))

	t.Run("main", func(t *testing.T) {
		main := make(chan message[int], 1)
		main <- message[int]{payload: 1}

		c, establish, liveness := session.NewClient(ctx, cl, loc)
		out := session.Connect(c, establish, main, liveness, inject[int])

		// (1) Establish -> payload -> Closed

		e := assertx.Element(t, out)
		assert.True(t, e.msg.IsEstablish())

		one := assertx.Element(t, out)
		assertx.Equal(t, one.payload, 1)

		close(main)

		cl := assertx.Element(t, out)
		assert.True(t, cl.msg.IsClosed())

		assertx.NoElement(t, out)
	})

	t.Run("manual", func(t *testing.T) {
		main := make(chan message[int], 2)
		main <- message[int]{payload: 1}

		c, establish, liveness := session.NewClient(ctx, cl, loc)
		out := session.Connect(c, establish, main, liveness, inject[int])

		// (1) Establish -> payload -> Closed

		e := assertx.Element(t, out)
		assert.True(t, e.msg.IsEstablish())

		one := assertx.Element(t, out)
		assertx.Equal(t, one.payload, 1)

		c.Close()

		cl := assertx.Element(t, out)
		assert.True(t, cl.msg.IsClosed())

		// (2) Once closed, the main message go nowhere

		assertx.NoElement(t, out)

		main <- message[int]{payload: 2}

		assertx.NoElement(t, out)
	})
}

func TestReceive(t *testing.T) {
	ctx := context.Background()
	cl := mockclock.NewUnsynchronized()
	cl.Set(time.Now())

	loc := location.NewInstance(location.New("us-west2", "unknown"))

	t.Run("main", func(t *testing.T) {
		main := make(chan message[int], 1)

		s, liveness := session.NewServer(ctx, cl, loc)
		s.Observe(ctx, session.NewEstablishMessage("sid", loc))

		out := session.Receive(s, main, liveness, inject[int])

		// (1) Established -> payload -> Closed

		e := assertx.Element(t, out)
		assert.True(t, e.msg.IsEstablished())

		main <- message[int]{payload: 1}

		one := assertx.Element(t, out)
		assertx.Equal(t, one.payload, 1)

		close(main)

		cl := assertx.Element(t, out)
		assert.True(t, cl.msg.IsClosed())

		assertx.NoElement(t, out)
	})

	t.Run("manual", func(t *testing.T) {
		main := make(chan message[int], 2)

		s, liveness := session.NewServer(ctx, cl, loc)
		s.Observe(ctx, session.NewEstablishMessage("sid", loc))

		out := session.Receive(s, main, liveness, inject[int])

		// (1) Establish -> payload -> Closed

		e := assertx.Element(t, out)
		assert.True(t, e.msg.IsEstablished())

		main <- message[int]{payload: 1}

		one := assertx.Element(t, out)
		assertx.Equal(t, one.payload, 1)

		s.Close()

		cl := assertx.Element(t, out)
		assert.True(t, cl.msg.IsClosed())

		// (2) Once closed, the main message go nowhere

		assertx.NoElement(t, out)

		main <- message[int]{payload: 2}

		assertx.NoElement(t, out)
	})
}

type message[T any] struct {
	msg     session.Message
	payload T
}

func inject[T any](msg session.Message) message[T] {
	return message[T]{msg: msg}
}
