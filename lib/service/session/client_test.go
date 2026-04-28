package session_test

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"

	"go.atoms.co/splitter/lib/service/location"
	"go.atoms.co/splitter/lib/service/session"
	"go.atoms.co/lib/testing/requirex"
)

func TestClient_Establish(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()

		def := location.NewInstance(location.New("us-west2", "unknown"))
		client, msg, _ := session.NewClient(ctx, def)
		defer client.Close()
		time.Sleep(100 * time.Millisecond)

		// Initial message should be an Establish message
		establish, ok := msg.Establish()
		assert.True(t, ok)
		assert.Equal(t, def, establish.Client)
	})
}

func TestClient_Heartbeat(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()

		def := location.NewInstance(location.New("us-west2", "pod1"))
		client, _, out := session.NewClient(ctx, def)
		defer client.Close()

		client.Observe(ctx, session.NewEstablishedMessage(time.Now().Add(30*time.Second), location.NewInstance(location.New("us-west2", "pod2"))))
		time.Sleep(100 * time.Millisecond)

		for i := 0; i < 4; i++ {
			time.Sleep(5 * time.Second)
			msg := requirex.Element(t, out)
			heartbeat, ok := msg.Heartbeat()
			assert.True(t, ok)
			//assert.Equal(t, cl.Now().UTC(), heartbeat)
			client.Observe(ctx, session.NewHeartbeakAckMessage(heartbeat.Add(30*time.Second)))
		}
	})
}

func TestClient_HeartbeatWithOption(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()

		def := location.NewInstance(location.New("us-west2", "pod1"))
		client, _, out := session.NewClient(ctx, def, session.WithClientHeartbeatDuration(30*time.Second), session.WithClientKeepAliveTimeout(60*time.Second))
		defer client.Close()

		client.Observe(ctx, session.NewEstablishedMessage(time.Now().Add(60*time.Second), location.NewInstance(location.New("us-west2", "pod2"))))

		for i := 0; i < 4; i++ {
			time.Sleep(30 * time.Second)
			msg := requirex.Element(t, out)
			heartbeat, ok := msg.Heartbeat()
			assert.True(t, ok)
			//assert.Equal(t, cl.Now().UTC(), heartbeat)
			client.Observe(ctx, session.NewHeartbeakAckMessage(heartbeat.Add(60*time.Second)))
		}
	})
}

func TestClient_ExpirationPending(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()

		def := location.NewInstance(location.New("us-west2", "unknown"))
		client, _, _ := session.NewClient(ctx, def)
		defer client.Close()
		time.Sleep(100 * time.Millisecond)

		// Not timed out yet
		time.Sleep(5 * time.Second)
		assert.False(t, client.IsClosed())

		// Timed out
		time.Sleep(10 * time.Second)
		assert.True(t, client.IsClosed())
	})
}

func TestClient_ExpirationPendingWithOption(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()

		def := location.NewInstance(location.New("us-west2", "unknown"))
		client, _, _ := session.NewClient(ctx, def, session.WithClientHeartbeatDuration(30*time.Second), session.WithClientKeepAliveTimeout(60*time.Second))
		defer client.Close()
		time.Sleep(100 * time.Millisecond)

		// Not timed out yet
		time.Sleep(30 * time.Second)
		assert.False(t, client.IsClosed())

		// Timed out
		time.Sleep(60 * time.Second)
		assert.True(t, client.IsClosed())
	})
}

func TestClient_ExpirationEstablished(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()

		def := location.NewInstance(location.New("us-west2", "pod1"))
		client, _, out := session.NewClient(ctx, def)
		defer client.Close()

		client.Observe(ctx, session.NewEstablishedMessage(time.Now().Add(30*time.Second), location.NewInstance(location.New("us-west2", "pod2"))))

		time.Sleep(5 * time.Second)
		msg := requirex.Element(t, out) // heartbeat
		heartbeat, _ := msg.Heartbeat()
		client.Observe(ctx, session.NewHeartbeakAckMessage(heartbeat.Add(30*time.Second)))

		time.Sleep(5 * time.Second)
		requirex.Element(t, out) // heartbeat 2

		// Not timed out yet
		time.Sleep(15 * time.Second)
		assert.False(t, client.IsClosed())

		// Timed out
		time.Sleep(15 * time.Second)
		assert.True(t, client.IsClosed())
	})
}

func TestClient_ExpirationEstablishedWithOption(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()

		def := location.NewInstance(location.New("us-west2", "pod1"))
		client, _, out := session.NewClient(ctx, def, session.WithClientHeartbeatDuration(30*time.Second), session.WithClientKeepAliveTimeout(60*time.Second))
		defer client.Close()

		client.Observe(ctx, session.NewEstablishedMessage(time.Now().Add(60*time.Second), location.NewInstance(location.New("us-west2", "pod2"))))

		time.Sleep(30 * time.Second)
		msg := requirex.Element(t, out) // heartbeat
		heartbeat, _ := msg.Heartbeat()
		client.Observe(ctx, session.NewHeartbeakAckMessage(heartbeat.Add(60*time.Second)))

		time.Sleep(30 * time.Second)
		requirex.Element(t, out) // heartbeat 2

		// Not timed out yet
		time.Sleep(25 * time.Second)
		assert.False(t, client.IsClosed())

		// Timed out
		time.Sleep(10 * time.Second)
		assert.True(t, client.IsClosed())
	})
}
