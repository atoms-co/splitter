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

func TestServer_Established(t *testing.T) {
	ctx := context.Background()
	cl := mockclock.NewUnsynchronized()

	instance := location.NewInstance(location.New("us-west2", "pod1"))
	server, out := session.NewServer(ctx, cl, instance)
	defer server.Close()

	// No startup messages
	dontRead(t, out)

	// First message should be an Established message after receiving an Establish
	server.Observe(ctx, session.NewEstablishMessage(session.NewID(), location.NewInstance(location.New("us-west2", "pod2"))))
	msg := read(t, out)
	established, ok := msg.Established()
	assert.True(t, ok)
	assert.Equal(t, cl.Now().Add(10*time.Second).UTC(), established.Ttl)
	assert.Equal(t, instance.ID(), established.Server.ID())
}

func TestServer_Heartbeat(t *testing.T) {
	ctx := context.Background()
	cl := mockclock.NewUnsynchronized()

	server, out := session.NewServer(ctx, cl, location.NewInstance(location.New("us-west2", "pod1")))
	defer server.Close()

	server.Observe(ctx, session.NewEstablishMessage(session.NewID(), location.NewInstance(location.New("us-west2", "pod2"))))
	read(t, out) // Established

	server.Observe(ctx, session.NewHeartbeatMessage(cl.Now()))
	msg := read(t, out)
	ttl, ok := msg.HeartbeatAck()
	assert.True(t, ok)
	assert.Equal(t, cl.Now().Add(10*time.Second).UTC(), ttl)
}

func TestServer_ExpirationPending(t *testing.T) {
	ctx := context.Background()
	cl := mockclock.NewUnsynchronized()

	server, _ := session.NewServer(ctx, cl, location.NewInstance(location.New("us-west2", "pod1")))
	defer server.Close()

	time.Sleep(100 * time.Millisecond)
	cl.Add(4 * time.Second)
	time.Sleep(100 * time.Millisecond)
	assert.False(t, server.IsClosed())

	cl.Add(4 * time.Second)
	time.Sleep(100 * time.Millisecond)
	assert.True(t, server.IsClosed())
}

func TestServer_ExpirationEstablished(t *testing.T) {
	ctx := context.Background()
	cl := mockclock.NewUnsynchronized()

	server, out := session.NewServer(ctx, cl, location.NewInstance(location.New("us-west2", "pod1")))
	defer server.Close()

	server.Observe(ctx, session.NewEstablishMessage(session.NewID(), location.NewInstance(location.New("us-west2", "unknown"))))
	read(t, out) // Establish

	server.Observe(ctx, session.NewHeartbeatMessage(cl.Now()))
	read(t, out) // HeartbeatAck

	cl.Add(8 * time.Second)
	time.Sleep(100 * time.Millisecond)
	assert.False(t, server.IsClosed())

	cl.Add(8 * time.Second) // 16s > 10s keepalive
	time.Sleep(100 * time.Millisecond)
	assert.True(t, server.IsClosed())
}

func dontRead(t *testing.T, ch <-chan session.Message) {
	t.Helper()
	for {
		select {
		case msg := <-ch:
			t.Fatalf("unexpected message: %v", msg)
		case <-time.After(1 * time.Second):
			return
		}
	}
}
