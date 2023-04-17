package session_test

import (
	"context"
	"go.atoms.co/splitter/lib/service/session"
	"go.atoms.co/lib/testing/mockclock"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

// TODO(jhhurwitz): 04/11/2023 Test with Synchronized mock to avoid sleeps

func TestClient_Establish(t *testing.T) {
	ctx := context.Background()
	cl := mockclock.NewUnsynchronized()

	def := session.NewClientDefinition(session.NewLocation("us-west2", "unknown"))
	client, msg, _ := session.NewClient(ctx, cl, def)
	defer client.Close()
	time.Sleep(100 * time.Millisecond)

	// Initial message should be an Establish message
	establish, ok := msg.Establish()
	assert.True(t, ok)
	assert.Equal(t, def, establish.Definition)
}

func TestClient_Heartbeat(t *testing.T) {
	ctx := context.Background()
	cl := mockclock.NewUnsynchronized()

	def := session.NewClientDefinition(session.NewLocation("us-west2", "unknown"))
	client, _, out := session.NewClient(ctx, cl, def)
	defer client.Close()

	client.Observe(ctx, session.NewEstablishedMessage(cl.Now().Add(30*time.Second)))
	time.Sleep(100 * time.Millisecond)

	for i := 0; i < 4; i++ {
		cl.Add(10 * time.Second)
		time.Sleep(100 * time.Millisecond)
		msg := read(t, out)
		heartbeat, ok := msg.Heartbeat()
		assert.True(t, ok)
		assert.Equal(t, cl.Now().UTC(), heartbeat)
		client.Observe(ctx, session.NewEstablishedMessage(heartbeat.Add(30*time.Second)))
	}
}

func TestClient_ExpirationPending(t *testing.T) {
	ctx := context.Background()
	cl := mockclock.NewUnsynchronized()

	def := session.NewClientDefinition(session.NewLocation("us-west2", "unknown"))
	client, _, _ := session.NewClient(ctx, cl, def)
	defer client.Close()
	time.Sleep(100 * time.Millisecond)

	// Not timed out yet
	cl.Add(10 * time.Second)
	time.Sleep(100 * time.Millisecond)
	assert.False(t, client.IsClosed())

	// Timed out
	cl.Add(10 * time.Second)
	time.Sleep(100 * time.Millisecond)
	assert.True(t, client.IsClosed())
}

func TestClient_ExpirationEstablished(t *testing.T) {
	ctx := context.Background()
	cl := mockclock.NewUnsynchronized()

	def := session.NewClientDefinition(session.NewLocation("us-west2", "unknown"))
	client, _, out := session.NewClient(ctx, cl, def)
	defer client.Close()

	client.Observe(ctx, session.NewEstablishedMessage(cl.Now().Add(30*time.Second)))
	time.Sleep(100 * time.Millisecond)

	cl.Add(10 * time.Second)
	time.Sleep(100 * time.Millisecond)
	msg := read(t, out) // heartbeat
	heartbeat, _ := msg.Heartbeat()
	client.Observe(ctx, session.NewEstablishedMessage(heartbeat.Add(30*time.Second)))

	cl.Add(10 * time.Second)
	time.Sleep(100 * time.Millisecond)
	read(t, out) // heartbeat 2

	// Not timed out yet
	cl.Add(15 * time.Second)
	time.Sleep(100 * time.Millisecond)
	assert.False(t, client.IsClosed())

	// Timed out
	cl.Add(15 * time.Second)
	time.Sleep(100 * time.Millisecond)
	assert.True(t, client.IsClosed())
}

func read(t *testing.T, ch <-chan session.Message) session.Message {
	t.Helper()
	for {
		select {
		case msg := <-ch:
			return msg
		case <-time.After(100 * time.Millisecond):
			t.Fatal("no message read")
		}
	}
}
