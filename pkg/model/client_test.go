package model_test

import (
	"context"
	"go.atoms.co/lib/iox"
	"go.atoms.co/splitter/pkg/model"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestCluster_WaitForActive(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		active := iox.NewAsyncCloser()
		active.Close()
		err := model.WaitForActive(context.Background(), &testOwnership{
			active:  active,
			revoked: iox.NewAsyncCloser(),
			expired: iox.NewAsyncCloser(),
		})
		assert.NoError(t, err)
	})

	t.Run("cancel", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := model.WaitForActive(ctx, &testOwnership{
			active:  iox.NewAsyncCloser(),
			revoked: iox.NewAsyncCloser(),
			expired: iox.NewAsyncCloser(),
		})
		assert.Error(t, err)
		assert.Equal(t, err, ctx.Err())
	})

	t.Run("revoked", func(t *testing.T) {
		revoked := iox.NewAsyncCloser()
		revoked.Close()
		err := model.WaitForActive(context.Background(), &testOwnership{
			active:  iox.NewAsyncCloser(),
			revoked: revoked,
			expired: iox.NewAsyncCloser(),
		})
		assert.Error(t, err)
		assert.Equal(t, err, model.ErrRevoked)
	})

	t.Run("expired", func(t *testing.T) {
		expired := iox.NewAsyncCloser()
		expired.Close()
		err := model.WaitForActive(context.Background(), &testOwnership{
			active:  iox.NewAsyncCloser(),
			revoked: iox.NewAsyncCloser(),
			expired: expired,
		})
		assert.Error(t, err)
		assert.Equal(t, err, model.ErrExpired)
	})
}

func TestCluster_WaitForRevoke(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		revoked := iox.NewAsyncCloser()
		revoked.Close()
		err := model.WaitForRevoke(context.Background(), &testOwnership{
			revoked: revoked,
			expired: iox.NewAsyncCloser(),
		})
		assert.NoError(t, err)
	})

	t.Run("cancel", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := model.WaitForRevoke(ctx, &testOwnership{
			revoked: iox.NewAsyncCloser(),
			expired: iox.NewAsyncCloser(),
		})
		assert.Error(t, err)
		assert.Equal(t, err, ctx.Err())
	})

	t.Run("expired", func(t *testing.T) {
		expired := iox.NewAsyncCloser()
		expired.Close()
		err := model.WaitForRevoke(context.Background(), &testOwnership{
			revoked: iox.NewAsyncCloser(),
			expired: expired,
		})
		assert.Error(t, err)
		assert.Equal(t, err, model.ErrExpired)
	})
}

type testOwnership struct {
	active  iox.AsyncCloser
	revoked iox.AsyncCloser
	expired iox.AsyncCloser
}

func (t *testOwnership) Active() <-chan struct{} {
	return t.active.Closed()
}

func (t *testOwnership) Revoked() <-chan struct{} {
	return t.revoked.Closed()
}

func (t *testOwnership) Expired() <-chan struct{} {
	return t.expired.Closed()
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
