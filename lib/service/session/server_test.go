package session_test

import (
	"context"
	"go.atoms.co/splitter/lib/service/location"
	"go.atoms.co/splitter/lib/service/session"
	"go.atoms.co/lib/testing/mockclock"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

// TODO(jhhurwitz): 04/11/2023 Test with Synchronized mock to avoid sleeps

func TestServer_Established(t *testing.T) {
	ctx := context.Background()
	cl := mockclock.NewUnsynchronized()

	server, out := session.NewServer(ctx, cl)
	defer server.Close()

	// No startup messages
	dontRead(t, out)

	// First message should be an Established message after receiving an Establish
	server.Observe(ctx, session.NewEstablishMessage(session.NewID(), session.NewClientDefinition(location.New("us-west2", "unknown"))))
	msg := read(t, out)
	established, ok := msg.Established()
	assert.True(t, ok)
	assert.Equal(t, cl.Now().Add(30*time.Second).UTC(), established)
}

func TestServer_Heartbeat(t *testing.T) {
	ctx := context.Background()
	cl := mockclock.NewUnsynchronized()

	server, out := session.NewServer(ctx, cl)
	defer server.Close()

	server.Observe(ctx, session.NewEstablishMessage(session.NewID(), session.NewClientDefinition(location.New("us-west2", "unknown"))))
	read(t, out) // Establish

	server.Observe(ctx, session.NewHeartbeatMessage(cl.Now()))
	msg := read(t, out)
	established, ok := msg.Established()
	assert.True(t, ok)
	assert.Equal(t, cl.Now().Add(30*time.Second).UTC(), established)
}

func TestServer_ExpirationPending(t *testing.T) {
	ctx := context.Background()
	cl := mockclock.NewUnsynchronized()

	server, _ := session.NewServer(ctx, cl)
	defer server.Close()

	time.Sleep(100 * time.Millisecond)
	cl.Add(10 * time.Second)
	time.Sleep(100 * time.Millisecond)
	assert.False(t, server.IsClosed())

	cl.Add(10 * time.Second)
	time.Sleep(100 * time.Millisecond)
	assert.True(t, server.IsClosed())
}

func TestServer_ExpirationEstablished(t *testing.T) {
	ctx := context.Background()
	cl := mockclock.NewUnsynchronized()

	server, out := session.NewServer(ctx, cl)
	defer server.Close()

	server.Observe(ctx, session.NewEstablishMessage(session.NewID(), session.NewClientDefinition(location.New("us-west2", "unknown"))))
	read(t, out) // Establish

	server.Observe(ctx, session.NewHeartbeatMessage(cl.Now()))
	read(t, out)

	cl.Add(15 * time.Second)
	time.Sleep(100 * time.Millisecond)
	assert.False(t, server.IsClosed())

	cl.Add(15 * time.Second)
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
