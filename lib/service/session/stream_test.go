package session_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.atoms.co/splitter/lib/service/location"
	"go.atoms.co/splitter/lib/service/session"
	"go.atoms.co/lib/testing/assertx"
	"go.atoms.co/lib/testing/mockclock"
	"go.atoms.co/lib/testing/requirex"
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

	t.Run("main", func(t *testing.T) {
		main := make(chan message[int], 1)

		msg := session.NewEstablishMessage("sid", client)
		establish, _ := msg.Establish()
		s, liveness, msg := session.NewServer(ctx, cl, instance, establish)
		defer s.Close()

		// Established -> payload -> Closed

		assert.True(t, msg.IsEstablished())

		out := session.Receive(s, main, liveness, inject[int])

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

		msg := session.NewEstablishMessage("sid", client)
		establish, _ := msg.Establish()
		s, liveness, msg := session.NewServer(ctx, cl, instance, establish)
		defer s.Close()

		out := session.Receive(s, main, liveness, inject[int])

		// Established -> payload -> Closed

		assert.True(t, msg.IsEstablished())

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

func TestReadEstablish(t *testing.T) {
	t.Run("timeout waiting", func(t *testing.T) {
		in := make(chan session.Message)
		close(in)
		_, err := session.ReadEstablish(in, func(msg session.Message) (session.Message, bool) {
			return msg, true
		})
		requirex.Equal(t, err, fmt.Errorf("no first session message"))
	})

	t.Run("not session message", func(t *testing.T) {
		in := make(chan session.Message, 1)
		in <- session.Message{}
		_, err := session.ReadEstablish(in, func(msg session.Message) (session.Message, bool) {
			return msg, false
		})
		requirex.Equal(t, err, fmt.Errorf("expected session message, got %v", session.Message{}))
	})

	t.Run("not establish session message", func(t *testing.T) {
		in := make(chan session.Message, 1)
		hb := session.NewHeartbeatMessage(time.Now())
		in <- hb
		_, err := session.ReadEstablish(in, func(msg session.Message) (session.Message, bool) {
			return msg, true
		})
		requirex.Equal(t, err, fmt.Errorf("expected establish session message, got %v", hb))
	})

	t.Run("established session", func(t *testing.T) {
		in := make(chan session.Message, 1)
		in <- session.NewEstablishMessage("sid", client)
		established, err := session.ReadEstablish(in, func(msg session.Message) (session.Message, bool) {
			return msg, true
		})
		require.NoError(t, err)
		requirex.Equal(t, established.Client, client)
		requirex.Equal(t, established.ID, "sid")
	})
}

type message[T any] struct {
	msg     session.Message
	payload T
}

func inject[T any](msg session.Message) message[T] {
	return message[T]{msg: msg}
}
