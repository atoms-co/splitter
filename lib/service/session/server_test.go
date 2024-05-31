package session_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.atoms.co/splitter/lib/service/location"
	"go.atoms.co/splitter/lib/service/session"
	"go.atoms.co/lib/testing/mockclock"
	"go.atoms.co/lib/testing/requirex"
)

var (
	client   = location.NewInstance(location.New("us-west1", "pod2"))
	instance = location.NewInstance(location.New("us-west2", "pod1"))
)

func TestServer_Established(t *testing.T) {
	ctx := context.Background()
	cl := mockclock.NewUnsynchronized()

	t.Run("established session", func(t *testing.T) {
		msg := session.NewEstablishMessage("sid", client)
		establish, _ := msg.Establish()
		server, out, msg := session.NewServer(ctx, cl, instance, establish)
		defer server.Close()

		requirex.ChanEmpty(t, out)

		established, _ := msg.Established()
		requirex.Equal(t, established.Server, instance)
		requirex.Equal(t, established.Ttl, cl.Now().Add(10*time.Second).UTC())
	})
}

func TestServer_Heartbeat(t *testing.T) {
	ctx := context.Background()
	cl := mockclock.NewUnsynchronized()

	msg := session.NewEstablishMessage("sid", client)
	establish, _ := msg.Establish()
	server, out, _ := session.NewServer(ctx, cl, instance, establish)
	defer server.Close()

	server.Observe(ctx, session.NewHeartbeatMessage(cl.Now()))

	msg = requirex.Element(t, out)
	ttl, ok := msg.HeartbeatAck()
	require.True(t, ok)
	requirex.Equal(t, cl.Now().Add(10*time.Second).UTC(), ttl)
}

func TestServer_ExpirationPending(t *testing.T) {
	ctx := context.Background()
	cl := mockclock.New(t, 1)

	msg := session.NewEstablishMessage("sid", client)
	establish, _ := msg.Establish()
	server, _, _ := session.NewServer(ctx, cl, instance, establish)
	defer server.Close()

	cl.Add(6 * time.Second)
	assert.False(t, server.IsClosed())

	cl.Add(6 * time.Second)
	assert.True(t, server.IsClosed())
}

func TestServer_CloseOnSecondEstablish(t *testing.T) {
	ctx := context.Background()
	cl := mockclock.NewUnsynchronized()

	in := make(chan session.Message, 1)
	in <- session.NewEstablishMessage("sid", client)
	msg := session.NewEstablishMessage("sid", client)
	establish, _ := msg.Establish()
	server, _, _ := session.NewServer(ctx, cl, instance, establish)
	defer server.Close()

	server.Observe(ctx, session.NewEstablishMessage("sid", client))
	requirex.Closed(t, server.Closed())
}

func TestServer_CloseOnClose(t *testing.T) {
	ctx := context.Background()
	cl := mockclock.NewUnsynchronized()

	msg := session.NewEstablishMessage("sid", client)
	establish, _ := msg.Establish()
	server, _, _ := session.NewServer(ctx, cl, instance, establish)
	defer server.Close()

	server.Observe(ctx, session.NewClosedMessage())
	requirex.Closed(t, server.Closed())
}
