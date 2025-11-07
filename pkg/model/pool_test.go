package model_test

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"atoms.co/lib-go/pkg/clock"
	"go.atoms.co/splitter/lib/service/location"
	"go.atoms.co/lib/testing/assertx"
	"go.atoms.co/lib/testing/synctestx"
	"go.atoms.co/lib/iox"
	"go.atoms.co/splitter/pkg/model"
)

func TestPeeredConnectionCache(t *testing.T) {
	self := model.NewInstance(location.NewInstance(location.New("us", "a")), "self")
	foo := model.NewInstance(location.NewInstance(location.New("us", "a")), "foo")
	bar := model.NewInstance(location.NewInstance(location.New("us", "b")), "bar")
	baz := model.NewInstance(location.NewInstance(location.New("us", "c")), "baz")

	synctestx.Run(t, "peered", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		dialer := newFakeDialer()
		cache := model.NewPeeredConnectionCache[int](ctx, clock.New(), self.ID(), dialer.dial)

		assertx.Equal(t, dialer.count, 0)

		// (1) Connections are delayed, not immediately dialed (excluding self).

		cache.Update(ctx, []model.Instance{self, foo, bar})

		assertx.Equal(t, dialer.count, 0)

		time.Sleep(40 * time.Second)

		assertx.Equal(t, dialer.count, 2)

		_, err := cache.Resolve(ctx, self)
		assertx.Equal(t, err, model.ErrNoResolution)
		_, err = cache.Resolve(ctx, foo)
		assert.NoError(t, err)
		_, err = cache.Resolve(ctx, bar)
		assert.NoError(t, err)

		assertx.Equal(t, dialer.count, 2)

		// (2) Additional lookup create ad-hoc connection.

		_, err = cache.Resolve(ctx, baz)
		assert.NoError(t, err)

		assertx.Equal(t, dialer.count, 3)
	})

	synctestx.Run(t, "gc", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		dialer := newFakeDialer()
		cache := model.NewPeeredConnectionCache[int](ctx, clock.New(), self.ID(), dialer.dial)

		// (1) Peered connections live indefinitely

		cache.Update(ctx, []model.Instance{foo})
		assertx.Equal(t, dialer.count, 0)

		time.Sleep(40 * time.Second)

		assertx.Equal(t, dialer.count, 1)

		time.Sleep(5 * time.Minute)

		_, err := cache.Resolve(ctx, foo)
		assert.NoError(t, err)

		assertx.Equal(t, dialer.count, 1)
		assert.False(t, dialer.con[foo.Endpoint()].IsClosed())

		// (2) If removed, they are cleared after 2 min

		cache.Update(ctx, []model.Instance{})
		assertx.Equal(t, dialer.count, 1)

		time.Sleep(time.Minute)

		_, err = cache.Resolve(ctx, foo)
		assert.NoError(t, err)
		assertx.Equal(t, dialer.count, 1)

		time.Sleep(5 * time.Minute)

		assert.True(t, dialer.con[foo.Endpoint()].IsClosed())

		// (3) It then reverts to ad-hoc status

		_, err = cache.Resolve(ctx, foo)
		assert.NoError(t, err)
		assertx.Equal(t, dialer.count, 2)
	})

	synctestx.Run(t, "adhoc context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		dialer := newFakeDialer()
		cache := model.NewPeeredConnectionCache[int](ctx, clock.New(), self.ID(), dialer.dial)

		assertx.Equal(t, dialer.count, 0)

		wctx, wcancel := context.WithCancel(ctx)

		_, err := cache.Resolve(wctx, foo)
		assert.NoError(t, err)

		assertx.Equal(t, dialer.count, 1)
		assert.False(t, dialer.con[foo.Endpoint()].IsClosed())

		wcancel()

		time.Sleep(50 * time.Millisecond)

		assert.False(t, dialer.con[foo.Endpoint()].IsClosed())
	})
}

type fakeDialer struct {
	con   map[string]iox.AsyncCloser
	count int
	mu    sync.Mutex
}

func newFakeDialer() *fakeDialer {
	return &fakeDialer{
		con: map[string]iox.AsyncCloser{},
	}
}

func (d *fakeDialer) dial(endpoint string) (io.Closer, int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	c := iox.NewAsyncCloser()
	d.con[endpoint] = c
	d.count++

	return closerShim{c: c}, d.count, nil
}

type closerShim struct {
	c iox.AsyncCloser
}

func (c closerShim) Close() error {
	c.c.Close()
	return nil
}
