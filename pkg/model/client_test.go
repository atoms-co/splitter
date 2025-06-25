package model_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"go.atoms.co/lib/iox"
	"go.atoms.co/splitter/pkg/model"
)

func TestClient_WaitForActive(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		active := iox.NewAsyncCloser()
		active.Close()
		err := model.WaitForActive(context.Background(), &testOwnership{
			active:          active,
			revoked:         iox.NewAsyncCloser(),
			revokeRequested: iox.NewAsyncCloser(),
			expired:         iox.NewAsyncCloser(),
		})
		assert.NoError(t, err)
	})

	t.Run("cancel", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := model.WaitForActive(ctx, &testOwnership{
			active:          iox.NewAsyncCloser(),
			revoked:         iox.NewAsyncCloser(),
			revokeRequested: iox.NewAsyncCloser(),
			expired:         iox.NewAsyncCloser(),
		})
		assert.Error(t, err)
		assert.Equal(t, err, ctx.Err())
	})

	t.Run("revoked", func(t *testing.T) {
		revoked := iox.NewAsyncCloser()
		revoked.Close()
		err := model.WaitForActive(context.Background(), &testOwnership{
			active:          iox.NewAsyncCloser(),
			revoked:         revoked,
			revokeRequested: iox.NewAsyncCloser(),
			expired:         iox.NewAsyncCloser(),
		})
		assert.Error(t, err)
		assert.Equal(t, err, model.ErrRevoked)
	})

	t.Run("expired", func(t *testing.T) {
		expired := iox.NewAsyncCloser()
		expired.Close()
		err := model.WaitForActive(context.Background(), &testOwnership{
			active:          iox.NewAsyncCloser(),
			revoked:         iox.NewAsyncCloser(),
			revokeRequested: iox.NewAsyncCloser(),
			expired:         expired,
		})
		assert.Error(t, err)
		assert.Equal(t, err, model.ErrExpired)
	})
}

func TestClient_WaitForRevoke(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		revoked := iox.NewAsyncCloser()
		revoked.Close()
		_, err := model.WaitForRevoke(context.Background(), &testOwnership{
			revoked:         revoked,
			revokeRequested: iox.NewAsyncCloser(),
			expired:         iox.NewAsyncCloser(),
		})
		assert.NoError(t, err)
	})

	t.Run("cancel", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := model.WaitForRevoke(ctx, &testOwnership{
			revoked:         iox.NewAsyncCloser(),
			revokeRequested: iox.NewAsyncCloser(),
			expired:         iox.NewAsyncCloser(),
		})
		assert.Error(t, err)
		assert.Equal(t, err, ctx.Err())
	})

	t.Run("expired", func(t *testing.T) {
		expired := iox.NewAsyncCloser()
		expired.Close()
		_, err := model.WaitForRevoke(context.Background(), &testOwnership{
			revoked:         iox.NewAsyncCloser(),
			revokeRequested: iox.NewAsyncCloser(),
			expired:         expired,
		})
		assert.Error(t, err)
		assert.Equal(t, err, model.ErrExpired)
	})
}

type testOwnership struct {
	active          iox.AsyncCloser
	revoked         iox.AsyncCloser
	revokeRequested iox.AsyncCloser
	expired         iox.AsyncCloser
	loader          *loader
	unloader        *unloader
}

func newTestOwnership() *testOwnership {
	return &testOwnership{
		active:          iox.NewAsyncCloser(),
		revoked:         iox.NewAsyncCloser(),
		expired:         iox.NewAsyncCloser(),
		revokeRequested: iox.NewAsyncCloser(),
		loader: &loader{
			unloaded: iox.NewAsyncCloser(),
			load:     iox.NewAsyncCloser(),
		},
		unloader: &unloader{
			loaded: iox.NewAsyncCloser(),
			unload: iox.NewAsyncCloser(),
		},
	}
}

func (t *testOwnership) Active() iox.RAsyncCloser {
	return t.active
}

func (t *testOwnership) Revoked() iox.RAsyncCloser {
	return t.revoked
}

func (t *testOwnership) RequestRevoke() {
	t.revokeRequested.Close()
}

type loader struct {
	unloaded iox.AsyncCloser
	load     iox.AsyncCloser
}

func (l *loader) Unloaded() iox.RAsyncCloser {
	return l.unloaded
}

func (l *loader) Load() {
	l.load.Close()
}

func (t *testOwnership) Loader() model.Loader {
	if t.loader != nil {
		return t.loader
	}
	return &loader{
		unloaded: iox.NewAsyncCloser(),
		load:     iox.NewAsyncCloser(),
	}
}

type unloader struct {
	loaded iox.AsyncCloser
	unload iox.AsyncCloser
}

func (u *unloader) Loaded() iox.RAsyncCloser {
	return u.loaded
}

func (u *unloader) Unload() {
	u.unload.Close()
}

func (t *testOwnership) Unloader() model.Unloader {
	if t.unloader != nil {
		return t.unloader
	}
	return &unloader{
		loaded: iox.NewAsyncCloser(),
		unload: iox.NewAsyncCloser(),
	}
}

func (t *testOwnership) Expired() iox.RAsyncCloser {
	return t.expired
}

func (t *testOwnership) IsActive() bool {
	return t.active.IsClosed()
}

func (t *testOwnership) IsRevoked() bool {
	return t.revoked.IsClosed()
}

func (t *testOwnership) IsExpired() bool {
	return t.expired.IsClosed()
}

func (t *testOwnership) Expiration() time.Time {
	return time.Time{}
}
