package session_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"go.atoms.co/splitter/lib/service/location"
	"go.atoms.co/splitter/lib/service/session"
	"go.atoms.co/lib/testing/mockclock"
)

// TODO(jhhurwitz): 04/11/2023 Test with Synchronized mock to avoid sleeps

func TestClient_Establish(t *testing.T) {
	ctx := context.Background()
	cl := mockclock.NewUnsynchronized()

	def := location.NewInstance(location.New("us-west2", "unknown"))
	client, msg, _ := session.NewClient(ctx, cl, def)
	defer client.Close()
	time.Sleep(100 * time.Millisecond)

	// Initial message should be an Establish message
	establish, ok := msg.Establish()
	assert.True(t, ok)
	assert.Equal(t, def, establish.Client)
}

func TestClient_Heartbeat(t *testing.T) {
	ctx := context.Background()
	cl := mockclock.NewUnsynchronized()

	def := location.NewInstance(location.New("us-west2", "pod1"))
	client, _, out := session.NewClient(ctx, cl, def)
	defer client.Close()

	client.Observe(ctx, session.NewEstablishedMessage(cl.Now().Add(30*time.Second), location.NewInstance(location.New("us-west2", "pod2"))))
	time.Sleep(100 * time.Millisecond)

	for i := 0; i < 4; i++ {
		cl.Add(5 * time.Second)
		time.Sleep(100 * time.Millisecond)
		msg := read(t, out)
		heartbeat, ok := msg.Heartbeat()
		assert.True(t, ok)
		assert.Equal(t, cl.Now().UTC(), heartbeat)
		client.Observe(ctx, session.NewHeartbeakAckMessage(heartbeat.Add(30*time.Second)))
	}
}

func TestClient_HeartbeatWithOption(t *testing.T) {
	ctx := context.Background()
	cl := mockclock.NewUnsynchronized()

	def := location.NewInstance(location.New("us-west2", "pod1"))
	client, _, out := session.NewClient(ctx, cl, def, session.WithClientHeartbeatDuration(30*time.Second), session.WithClientKeepAliveTimeout(60*time.Second))
	defer client.Close()

	client.Observe(ctx, session.NewEstablishedMessage(cl.Now().Add(60*time.Second), location.NewInstance(location.New("us-west2", "pod2"))))
	time.Sleep(100 * time.Millisecond)

	for i := 0; i < 4; i++ {
		cl.Add(30 * time.Second)
		time.Sleep(100 * time.Millisecond)
		msg := read(t, out)
		heartbeat, ok := msg.Heartbeat()
		assert.True(t, ok)
		assert.Equal(t, cl.Now().UTC(), heartbeat)
		client.Observe(ctx, session.NewHeartbeakAckMessage(heartbeat.Add(60*time.Second)))
	}
}

func TestClient_ExpirationPending(t *testing.T) {
	ctx := context.Background()
	cl := mockclock.NewUnsynchronized()

	def := location.NewInstance(location.New("us-west2", "unknown"))
	client, _, _ := session.NewClient(ctx, cl, def)
	defer client.Close()
	time.Sleep(100 * time.Millisecond)

	// Not timed out yet
	cl.Add(5 * time.Second)
	time.Sleep(100 * time.Millisecond)
	assert.False(t, client.IsClosed())

	// Timed out
	cl.Add(10 * time.Second)
	time.Sleep(100 * time.Millisecond)
	assert.True(t, client.IsClosed())
}

func TestClient_ExpirationPendingWithOption(t *testing.T) {
	ctx := context.Background()
	cl := mockclock.NewUnsynchronized()

	def := location.NewInstance(location.New("us-west2", "unknown"))
	client, _, _ := session.NewClient(ctx, cl, def, session.WithClientHeartbeatDuration(30*time.Second), session.WithClientKeepAliveTimeout(60*time.Second))
	defer client.Close()
	time.Sleep(100 * time.Millisecond)

	// Not timed out yet
	cl.Add(30 * time.Second)
	time.Sleep(100 * time.Millisecond)
	assert.False(t, client.IsClosed())

	// Timed out
	cl.Add(60 * time.Second)
	time.Sleep(100 * time.Millisecond)
	assert.True(t, client.IsClosed())
}

func TestClient_ExpirationEstablished(t *testing.T) {
	ctx := context.Background()
	cl := mockclock.NewUnsynchronized()

	def := location.NewInstance(location.New("us-west2", "pod1"))
	client, _, out := session.NewClient(ctx, cl, def)
	defer client.Close()

	client.Observe(ctx, session.NewEstablishedMessage(cl.Now().Add(30*time.Second), location.NewInstance(location.New("us-west2", "pod2"))))
	time.Sleep(100 * time.Millisecond)

	cl.Add(5 * time.Second)
	time.Sleep(100 * time.Millisecond)
	msg := read(t, out) // heartbeat
	heartbeat, _ := msg.Heartbeat()
	client.Observe(ctx, session.NewHeartbeakAckMessage(heartbeat.Add(30*time.Second)))

	cl.Add(5 * time.Second)
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

func TestClient_ExpirationEstablishedWithOption(t *testing.T) {
	ctx := context.Background()
	cl := mockclock.NewUnsynchronized()

	def := location.NewInstance(location.New("us-west2", "pod1"))
	client, _, out := session.NewClient(ctx, cl, def, session.WithClientHeartbeatDuration(30*time.Second), session.WithClientKeepAliveTimeout(60*time.Second))
	defer client.Close()

	client.Observe(ctx, session.NewEstablishedMessage(cl.Now().Add(60*time.Second), location.NewInstance(location.New("us-west2", "pod2"))))
	time.Sleep(100 * time.Millisecond)

	cl.Add(30 * time.Second)
	time.Sleep(100 * time.Millisecond)
	msg := read(t, out) // heartbeat
	heartbeat, _ := msg.Heartbeat()
	client.Observe(ctx, session.NewHeartbeakAckMessage(heartbeat.Add(60*time.Second)))

	cl.Add(30 * time.Second)
	time.Sleep(100 * time.Millisecond)
	read(t, out) // heartbeat 2

	// Not timed out yet
	cl.Add(25 * time.Second)
	time.Sleep(100 * time.Millisecond)
	assert.False(t, client.IsClosed())

	// Timed out
	cl.Add(10 * time.Second)
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
