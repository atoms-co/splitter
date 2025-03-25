package model_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"go.atoms.co/splitter/lib/service/location"
	"go.atoms.co/lib/testing/requirex"
	"go.atoms.co/slicex"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/testing/prefab"
)

var (
	cid = model.NewClusterID(location.NewInstance(location.Location{}), time.Now())
)

func TestHandle(t *testing.T) {
	grant := prefab.NewGrantInfo(t, "g1", "t/s/d", model.Global, "", "0", "a", model.LoadedGrantState)
	key := prefab.NewQDK(t, "t/s/d", "", "1")
	req := prefab.NewQDK(t, "t/s/d", "", "2")
	resp := prefab.NewQDK(t, "t/s/d", "", "3")

	ctx := context.Background()
	r := newFakeRange()

	t.Run("local active successful", func(t *testing.T) {
		proxy := newTestProxy()

		proxy.grants.Activate(grant.ID(), grant.Shard(), r)

		rt, err := model.Handle(ctx, proxy, key, remoteInvalid, req, localSuccess(resp))
		require.NoError(t, err)
		requirex.Equal(t, rt, resp)
	})

	t.Run("local active failing", func(t *testing.T) {
		proxy := newTestProxy()

		proxy.grants.Activate(grant.ID(), grant.Shard(), r)

		rt, err := model.Handle(ctx, proxy, key, remoteSuccess(resp), req, localFailure)
		requirex.Equal(t, err, model.ErrInvalid)
		requirex.Equal(t, rt, model.QualifiedDomainKey{})
	})

	t.Run("cluster not initialized", func(t *testing.T) {
		proxy := newTestProxy()

		rt, err := model.Handle(ctx, proxy, key, remoteSuccess(resp), req, localSuccess(resp))
		requirex.Equal(t, err.Error(), fmt.Sprintf("not initialized: %v", model.ErrNotOwned))
		requirex.Equal(t, rt, model.QualifiedDomainKey{})
	})

	t.Run("no owner in cluster", func(t *testing.T) {
		proxy := newTestProxy()

		cluster := model.NewClusterMap(cid, slicex.New(grant.Shard()))
		proxy.pool.Current = cluster

		rt, err := model.Handle(ctx, proxy, key, remoteSuccess(resp), req, localSuccess(resp))
		requirex.Equal(t, err.Error(), fmt.Sprintf("no owner: %v", model.ErrNotOwned))
		requirex.Equal(t, rt, model.QualifiedDomainKey{})
	})

	t.Run("local non-active successful", func(t *testing.T) {
		proxy := newTestProxy()

		cluster := newCluster(t, slicex.New(model.NewAssignment(prefab.Instance1, grant)), grant.Shard())
		proxy.pool.Current = cluster

		proxy.grants.Loaded(grant.ID(), grant.Shard(), r)

		rt, err := model.Handle(ctx, proxy, key, remoteInvalid, req, localSuccess(resp))
		require.NoError(t, err)
		requirex.Equal(t, rt, resp)
	})

	t.Run("local non-active failing", func(t *testing.T) {
		proxy := newTestProxy()

		cluster := newCluster(t, slicex.New(model.NewAssignment(prefab.Instance1, grant)), grant.Shard())
		proxy.pool.Current = cluster

		proxy.grants.Loaded(grant.ID(), grant.Shard(), r)

		rt, err := model.Handle(ctx, proxy, key, remoteSuccess(resp), req, localFailure)
		requirex.Equal(t, err, model.ErrInvalid)
		requirex.Equal(t, rt, model.QualifiedDomainKey{})
	})

	t.Run("resolve failed", func(t *testing.T) {
		proxy := newTestProxy()

		cluster := newCluster(t, slicex.New(model.NewAssignment(prefab.Instance1, grant)), grant.Shard())
		proxy.pool.Current = cluster
		proxy.pool.Failed[prefab.Instance1.ID()] = model.ErrNotFound

		rt, err := model.Handle(ctx, proxy, key, remoteSuccess(resp), req, localSuccess(resp))
		requirex.Equal(t, err, model.ErrNotFound)
		requirex.Equal(t, rt, model.QualifiedDomainKey{})
	})

	t.Run("remote successful", func(t *testing.T) {
		proxy := newTestProxy()

		cluster := newCluster(t, slicex.New(model.NewAssignment(prefab.Instance1, grant)), grant.Shard())
		proxy.pool.Current = cluster
		proxy.pool.Resolution[prefab.Instance1.ID()] = &grpc.ClientConn{}

		rt, err := model.Handle(ctx, proxy, key, remoteSuccess(resp), req, localFailure)
		require.NoError(t, err)
		requirex.Equal(t, rt, resp)
	})

	t.Run("remote failed", func(t *testing.T) {
		proxy := newTestProxy()

		cluster := newCluster(t, slicex.New(model.NewAssignment(prefab.Instance1, grant)), grant.Shard())
		proxy.pool.Current = cluster
		proxy.pool.Resolution[prefab.Instance1.ID()] = &grpc.ClientConn{}

		rt, err := model.Handle(ctx, proxy, key, remoteInvalid, req, localSuccess(resp))
		requirex.Equal(t, err, model.ToGRPCError(model.ErrInvalid))
		requirex.Equal(t, rt, model.QualifiedDomainKey{})
	})
}

func TestHandleEx(t *testing.T) {
	grant := prefab.NewGrantInfo(t, "g1", "t/s/d", model.Global, "", "0", "a", model.LoadedGrantState)
	key := prefab.NewQDK(t, "t/s/d", "", "1")
	req := prefab.NewQDK(t, "t/s/d", "", "2")
	resp := prefab.NewQDK(t, "t/s/d", "", "3")

	ctx := context.Background()
	r := newFakeRange()

	t.Run("local active successful", func(t *testing.T) {
		proxy := newTestProxy()

		proxy.grants.Activate(grant.ID(), grant.Shard(), r)

		rt, err := model.HandleEx(ctx, proxy, key, remoteInvalid, req, model.ToGRPCError, localSuccess(resp))
		require.NoError(t, err)
		requirex.Equal(t, rt, resp)
	})

	t.Run("local active failing", func(t *testing.T) {
		proxy := newTestProxy()

		proxy.grants.Activate(grant.ID(), grant.Shard(), r)

		rt, err := model.HandleEx(ctx, proxy, key, remoteSuccess(resp), req, model.ToGRPCError, localFailure)
		requirex.Equal(t, err, model.ToGRPCError(model.ErrInvalid))
		requirex.Equal(t, rt, model.QualifiedDomainKey{})
	})

	t.Run("cluster not initialized", func(t *testing.T) {
		proxy := newTestProxy()

		rt, err := model.HandleEx(ctx, proxy, key, remoteSuccess(resp), req, model.ToGRPCError, localSuccess(resp))
		requirex.Equal(t, err, model.ToGRPCError(fmt.Errorf("not initialized: %w", model.ErrNotOwned)))
		requirex.Equal(t, rt, model.QualifiedDomainKey{})
	})

	t.Run("no owner in cluster", func(t *testing.T) {
		proxy := newTestProxy()

		cluster := model.NewClusterMap(cid, slicex.New(grant.Shard()))
		proxy.pool.Current = cluster

		rt, err := model.HandleEx(ctx, proxy, key, remoteSuccess(resp), req, model.ToGRPCError, localSuccess(resp))
		requirex.Equal(t, err, model.ToGRPCError(fmt.Errorf("no owner: %w", model.ErrNotOwned)))
		requirex.Equal(t, rt, model.QualifiedDomainKey{})
	})

	t.Run("local non-active successful", func(t *testing.T) {
		proxy := newTestProxy()

		cluster := newCluster(t, slicex.New(model.NewAssignment(prefab.Instance1, grant)), grant.Shard())
		proxy.pool.Current = cluster

		proxy.grants.Loaded(grant.ID(), grant.Shard(), r)

		rt, err := model.HandleEx(ctx, proxy, key, remoteInvalid, req, model.ToGRPCError, localSuccess(resp))
		require.NoError(t, err)
		requirex.Equal(t, rt, resp)
	})

	t.Run("local non-active failing", func(t *testing.T) {
		proxy := newTestProxy()

		cluster := newCluster(t, slicex.New(model.NewAssignment(prefab.Instance1, grant)), grant.Shard())
		proxy.pool.Current = cluster

		proxy.grants.Loaded(grant.ID(), grant.Shard(), r)

		rt, err := model.HandleEx(ctx, proxy, key, remoteSuccess(resp), req, model.ToGRPCError, localFailure)
		requirex.Equal(t, err, model.ToGRPCError(model.ErrInvalid))
		requirex.Equal(t, rt, model.QualifiedDomainKey{})
	})

	t.Run("resolve failed", func(t *testing.T) {
		proxy := newTestProxy()

		cluster := newCluster(t, slicex.New(model.NewAssignment(prefab.Instance1, grant)), grant.Shard())
		proxy.pool.Current = cluster
		proxy.pool.Failed[prefab.Instance1.ID()] = model.ErrNotFound

		rt, err := model.HandleEx(ctx, proxy, key, remoteSuccess(resp), req, model.ToGRPCError, localSuccess(resp))
		requirex.Equal(t, err, model.ToGRPCError(model.ErrNotFound))
		requirex.Equal(t, rt, model.QualifiedDomainKey{})
	})

	t.Run("remote successful", func(t *testing.T) {
		proxy := newTestProxy()

		cluster := newCluster(t, slicex.New(model.NewAssignment(prefab.Instance1, grant)), grant.Shard())
		proxy.pool.Current = cluster
		proxy.pool.Resolution[prefab.Instance1.ID()] = &grpc.ClientConn{}

		rt, err := model.HandleEx(ctx, proxy, key, remoteSuccess(resp), req, model.ToGRPCError, localFailure)
		require.NoError(t, err)
		requirex.Equal(t, rt, resp)
	})

	t.Run("remote failed", func(t *testing.T) {
		proxy := newTestProxy()

		cluster := newCluster(t, slicex.New(model.NewAssignment(prefab.Instance1, grant)), grant.Shard())
		proxy.pool.Current = cluster
		proxy.pool.Resolution[prefab.Instance1.ID()] = &grpc.ClientConn{}

		rt, err := model.HandleEx(ctx, proxy, key, remoteInvalid, req, model.ToGRPCError, localSuccess(resp))
		requirex.Equal(t, err, model.ToGRPCError(model.ErrInvalid))
		requirex.Equal(t, rt, model.QualifiedDomainKey{})
	})
}

func TestHandleLocal(t *testing.T) {
	grant := prefab.NewGrantInfo(t, "g1", "t/s/d", model.Global, "", "0", "a", model.LoadedGrantState)
	key := prefab.NewQDK(t, "t/s/d", "", "1")
	req := prefab.NewQDK(t, "t/s/d", "", "2")
	resp := prefab.NewQDK(t, "t/s/d", "", "3")

	ctx := context.Background()
	r := newFakeRange()

	t.Run("found and successful", func(t *testing.T) {
		resolver := newTestGrantResolver()
		resolver.grants.Activate(grant.ID(), grant.Shard(), r)

		rt, err := model.HandleLocal(ctx, resolver, key, req, handler(resp))
		requirex.Equal(t, rt, resp)
		require.NoError(t, err)
	})

	t.Run("found and fails", func(t *testing.T) {
		resolver := newTestGrantResolver()
		resolver.grants.Activate(grant.ID(), grant.Shard(), r)

		rt, err := model.HandleLocal(ctx, resolver, key, req, errHandler)
		requirex.Equal(t, rt, model.QualifiedDomainKey{})
		requirex.Equal(t, err, model.ErrInvalid)
	})

	t.Run("not found", func(t *testing.T) {
		resolver := newTestGrantResolver()

		rt, err := model.HandleLocal(ctx, resolver, key, req, handler(resp))
		requirex.Equal(t, rt, model.QualifiedDomainKey{})
		requirex.Equal(t, err, model.ToGRPCError(model.ErrNotOwned))
	})
}

func TestHandleLocalEx(t *testing.T) {
	grant := prefab.NewGrantInfo(t, "g1", "t/s/d", model.Global, "", "0", "a", model.LoadedGrantState)
	key := prefab.NewQDK(t, "t/s/d", "", "1")
	req := prefab.NewQDK(t, "t/s/d", "", "2")
	resp := prefab.NewQDK(t, "t/s/d", "", "3")

	ctx := context.Background()
	r := newFakeRange()

	t.Run("found and successful", func(t *testing.T) {
		resolver := newTestGrantResolver()
		resolver.grants.Activate(grant.ID(), grant.Shard(), r)

		rt, err := model.HandleLocalEx(ctx, resolver, key, req, model.ToGRPCError, handler(resp))
		requirex.Equal(t, rt, resp)
		require.NoError(t, err)
	})

	t.Run("found and fails", func(t *testing.T) {
		resolver := newTestGrantResolver()
		resolver.grants.Activate(grant.ID(), grant.Shard(), r)

		rt, err := model.HandleLocalEx(ctx, resolver, key, req, model.ToGRPCError, errHandler)
		requirex.Equal(t, rt, model.QualifiedDomainKey{})
		requirex.Equal(t, err, model.ToGRPCError(model.ErrInvalid))
	})

	t.Run("not found", func(t *testing.T) {
		resolver := newTestGrantResolver()

		rt, err := model.HandleLocalEx(ctx, resolver, key, req, model.ToGRPCError, handler(resp))
		requirex.Equal(t, rt, model.QualifiedDomainKey{})
		requirex.Equal(t, err, model.ToGRPCError(model.ErrNotOwned))
	})
}

type testGrantResolver struct {
	grants *model.GrantMap[*fakeRange]
}

func newTestGrantResolver() *testGrantResolver {
	return &testGrantResolver{
		grants: model.NewGrantMap[*fakeRange](),
	}
}

func (r *testGrantResolver) Lookup(key model.QualifiedDomainKey, grants ...model.GrantState) (*fakeRange, bool) {
	return r.grants.Lookup(key, grants...)
}

func (r *testGrantResolver) DomainKey(key model.QualifiedDomainKey) model.QualifiedDomainKey {
	return key
}

func handler(resp model.QualifiedDomainKey) func(r *fakeRange, ctx context.Context, key model.QualifiedDomainKey) (model.QualifiedDomainKey, error) {
	return func(r *fakeRange, ctx context.Context, key model.QualifiedDomainKey) (model.QualifiedDomainKey, error) {
		return resp, nil
	}
}

func errHandler(r *fakeRange, ctx context.Context, key model.QualifiedDomainKey) (model.QualifiedDomainKey, error) {
	return model.QualifiedDomainKey{}, model.ErrInvalid
}

func remoteSuccess(resp model.QualifiedDomainKey) func(r int, ctx context.Context, key model.QualifiedDomainKey, opts ...grpc.CallOption) (model.QualifiedDomainKey, error) {
	return func(r int, ctx context.Context, key model.QualifiedDomainKey, opts ...grpc.CallOption) (model.QualifiedDomainKey, error) {
		return resp, nil
	}
}

func remoteInvalid(r int, ctx context.Context, key model.QualifiedDomainKey, opts ...grpc.CallOption) (model.QualifiedDomainKey, error) {
	return model.QualifiedDomainKey{}, model.ToGRPCError(model.ErrInvalid)
}

func localSuccess(resp model.QualifiedDomainKey) func(r *fakeRange) (model.QualifiedDomainKey, error) {
	return func(r *fakeRange) (model.QualifiedDomainKey, error) {
		return resp, nil
	}
}

func localFailure(r *fakeRange) (model.QualifiedDomainKey, error) {
	return model.QualifiedDomainKey{}, model.ErrInvalid
}

type testProxy struct {
	*testGrantResolver
	model.Resolver[int, model.QualifiedDomainKey]

	pool *fakePool
}

func newTestProxy() *testProxy {
	grantResolver := newTestGrantResolver()
	pool := newFakePool()
	resolver := model.NewResolver(pool, func(connInterface grpc.ClientConnInterface) int { return 0 })

	return &testProxy{
		testGrantResolver: grantResolver,
		Resolver:          resolver,

		pool: pool,
	}
}

func (r *testProxy) Cluster() (model.Cluster, bool) {
	return r.pool.Cluster()
}

func (r *testProxy) DomainKey(key model.QualifiedDomainKey) model.QualifiedDomainKey {
	return key
}
