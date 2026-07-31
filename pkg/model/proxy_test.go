package model

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"go.atoms.co/lib/testing/requirex"
	"go.atoms.co/slicex"
	"go.atoms.co/splitter/lib/service/location"
)

var (
	cid = NewClusterID(location.NewInstance(location.Location{}), time.Now())
)

func TestHandle(t *testing.T) {
	grant := prefab.NewGrantInfo(t, "g1", "t/s/d", Global, "", "0", "a", LoadedGrantState)
	key := prefab.NewQDK(t, "t/s/d", "", "1")
	req := prefab.NewQDK(t, "t/s/d", "", "2")
	resp := prefab.NewQDK(t, "t/s/d", "", "3")

	ctx := context.Background()
	r := newFakeRange()

	t.Run("local active successful", func(t *testing.T) {
		proxy := newTestProxy()

		proxy.grants.Activate(grant.ID(), grant.Shard(), r)

		rt, err := Handle(ctx, proxy, key, remoteInvalid, req, localSuccess(resp))
		require.NoError(t, err)
		requirex.Equal(t, rt, resp)
	})

	t.Run("local active failing", func(t *testing.T) {
		proxy := newTestProxy()

		proxy.grants.Activate(grant.ID(), grant.Shard(), r)

		rt, err := Handle(ctx, proxy, key, remoteSuccess(resp), req, localFailure)
		requirex.Equal(t, err, ErrInvalid)
		requirex.Equal(t, rt, QualifiedDomainKey{})
	})

	t.Run("cluster not initialized", func(t *testing.T) {
		proxy := newTestProxy()

		rt, err := Handle(ctx, proxy, key, remoteSuccess(resp), req, localSuccess(resp))
		requirex.Equal(t, err.Error(), fmt.Sprintf("not initialized: %v", ErrNotOwned))
		requirex.Equal(t, rt, QualifiedDomainKey{})
	})

	t.Run("no owner in cluster", func(t *testing.T) {
		proxy := newTestProxy()

		cluster := NewClusterMap(cid, slicex.New(grant.Shard()))
		proxy.pool.Current = cluster

		rt, err := Handle(ctx, proxy, key, remoteSuccess(resp), req, localSuccess(resp))
		requirex.Equal(t, err.Error(), fmt.Sprintf("no owner: %v", ErrNotOwned))
		requirex.Equal(t, rt, QualifiedDomainKey{})
	})

	t.Run("local non-active successful", func(t *testing.T) {
		proxy := newTestProxy()

		cluster := newCluster(t, slicex.New(NewAssignment(prefab.Instance1, grant)), grant.Shard())
		proxy.pool.Current = cluster

		proxy.grants.Loaded(grant.ID(), grant.Shard(), r)

		rt, err := Handle(ctx, proxy, key, remoteInvalid, req, localSuccess(resp))
		require.NoError(t, err)
		requirex.Equal(t, rt, resp)
	})

	t.Run("local non-active failing", func(t *testing.T) {
		proxy := newTestProxy()

		cluster := newCluster(t, slicex.New(NewAssignment(prefab.Instance1, grant)), grant.Shard())
		proxy.pool.Current = cluster

		proxy.grants.Loaded(grant.ID(), grant.Shard(), r)

		rt, err := Handle(ctx, proxy, key, remoteSuccess(resp), req, localFailure)
		requirex.Equal(t, err, ErrInvalid)
		requirex.Equal(t, rt, QualifiedDomainKey{})
	})

	t.Run("resolve failed", func(t *testing.T) {
		proxy := newTestProxy()

		cluster := newCluster(t, slicex.New(NewAssignment(prefab.Instance1, grant)), grant.Shard())
		proxy.pool.Current = cluster
		proxy.pool.Failed[prefab.Instance1.ID()] = ErrNotFound

		rt, err := Handle(ctx, proxy, key, remoteSuccess(resp), req, localSuccess(resp))
		requirex.Equal(t, err, ErrNotFound)
		requirex.Equal(t, rt, QualifiedDomainKey{})
	})

	t.Run("remote successful", func(t *testing.T) {
		proxy := newTestProxy()

		cluster := newCluster(t, slicex.New(NewAssignment(prefab.Instance1, grant)), grant.Shard())
		proxy.pool.Current = cluster
		proxy.pool.Resolution[prefab.Instance1.ID()] = &grpc.ClientConn{}

		rt, err := Handle(ctx, proxy, key, remoteSuccess(resp), req, localFailure)
		require.NoError(t, err)
		requirex.Equal(t, rt, resp)
	})

	t.Run("remote failed", func(t *testing.T) {
		proxy := newTestProxy()

		cluster := newCluster(t, slicex.New(NewAssignment(prefab.Instance1, grant)), grant.Shard())
		proxy.pool.Current = cluster
		proxy.pool.Resolution[prefab.Instance1.ID()] = &grpc.ClientConn{}

		rt, err := Handle(ctx, proxy, key, remoteInvalid, req, localSuccess(resp))
		requirex.Equal(t, err, ToGRPCError(ErrInvalid))
		requirex.Equal(t, rt, QualifiedDomainKey{})
	})
}

func TestHandleEx(t *testing.T) {
	grant := prefab.NewGrantInfo(t, "g1", "t/s/d", Global, "", "0", "a", LoadedGrantState)
	key := prefab.NewQDK(t, "t/s/d", "", "1")
	req := prefab.NewQDK(t, "t/s/d", "", "2")
	resp := prefab.NewQDK(t, "t/s/d", "", "3")

	ctx := context.Background()
	r := newFakeRange()

	t.Run("local active successful", func(t *testing.T) {
		proxy := newTestProxy()

		proxy.grants.Activate(grant.ID(), grant.Shard(), r)

		rt, err := HandleEx(ctx, proxy, key, remoteInvalid, req, ToGRPCError, localSuccess(resp))
		require.NoError(t, err)
		requirex.Equal(t, rt, resp)
	})

	t.Run("local active failing", func(t *testing.T) {
		proxy := newTestProxy()

		proxy.grants.Activate(grant.ID(), grant.Shard(), r)

		rt, err := HandleEx(ctx, proxy, key, remoteSuccess(resp), req, ToGRPCError, localFailure)
		requirex.Equal(t, err, ToGRPCError(ErrInvalid))
		requirex.Equal(t, rt, QualifiedDomainKey{})
	})

	t.Run("cluster not initialized", func(t *testing.T) {
		proxy := newTestProxy()

		rt, err := HandleEx(ctx, proxy, key, remoteSuccess(resp), req, ToGRPCError, localSuccess(resp))
		requirex.Equal(t, err, ToGRPCError(fmt.Errorf("not initialized: %w", ErrNotOwned)))
		requirex.Equal(t, rt, QualifiedDomainKey{})
	})

	t.Run("no owner in cluster", func(t *testing.T) {
		proxy := newTestProxy()

		cluster := NewClusterMap(cid, slicex.New(grant.Shard()))
		proxy.pool.Current = cluster

		rt, err := HandleEx(ctx, proxy, key, remoteSuccess(resp), req, ToGRPCError, localSuccess(resp))
		requirex.Equal(t, err, ToGRPCError(fmt.Errorf("no owner: %w", ErrNotOwned)))
		requirex.Equal(t, rt, QualifiedDomainKey{})
	})

	t.Run("local non-active successful", func(t *testing.T) {
		proxy := newTestProxy()

		cluster := newCluster(t, slicex.New(NewAssignment(prefab.Instance1, grant)), grant.Shard())
		proxy.pool.Current = cluster

		proxy.grants.Loaded(grant.ID(), grant.Shard(), r)

		rt, err := HandleEx(ctx, proxy, key, remoteInvalid, req, ToGRPCError, localSuccess(resp))
		require.NoError(t, err)
		requirex.Equal(t, rt, resp)
	})

	t.Run("local non-active failing", func(t *testing.T) {
		proxy := newTestProxy()

		cluster := newCluster(t, slicex.New(NewAssignment(prefab.Instance1, grant)), grant.Shard())
		proxy.pool.Current = cluster

		proxy.grants.Loaded(grant.ID(), grant.Shard(), r)

		rt, err := HandleEx(ctx, proxy, key, remoteSuccess(resp), req, ToGRPCError, localFailure)
		requirex.Equal(t, err, ToGRPCError(ErrInvalid))
		requirex.Equal(t, rt, QualifiedDomainKey{})
	})

	t.Run("resolve failed", func(t *testing.T) {
		proxy := newTestProxy()

		cluster := newCluster(t, slicex.New(NewAssignment(prefab.Instance1, grant)), grant.Shard())
		proxy.pool.Current = cluster
		proxy.pool.Failed[prefab.Instance1.ID()] = ErrNotFound

		rt, err := HandleEx(ctx, proxy, key, remoteSuccess(resp), req, ToGRPCError, localSuccess(resp))
		requirex.Equal(t, err, ToGRPCError(ErrNotFound))
		requirex.Equal(t, rt, QualifiedDomainKey{})
	})

	t.Run("remote successful", func(t *testing.T) {
		proxy := newTestProxy()

		cluster := newCluster(t, slicex.New(NewAssignment(prefab.Instance1, grant)), grant.Shard())
		proxy.pool.Current = cluster
		proxy.pool.Resolution[prefab.Instance1.ID()] = &grpc.ClientConn{}

		rt, err := HandleEx(ctx, proxy, key, remoteSuccess(resp), req, ToGRPCError, localFailure)
		require.NoError(t, err)
		requirex.Equal(t, rt, resp)
	})

	t.Run("remote failed", func(t *testing.T) {
		proxy := newTestProxy()

		cluster := newCluster(t, slicex.New(NewAssignment(prefab.Instance1, grant)), grant.Shard())
		proxy.pool.Current = cluster
		proxy.pool.Resolution[prefab.Instance1.ID()] = &grpc.ClientConn{}

		rt, err := HandleEx(ctx, proxy, key, remoteInvalid, req, ToGRPCError, localSuccess(resp))
		requirex.Equal(t, err, ToGRPCError(ErrInvalid))
		requirex.Equal(t, rt, QualifiedDomainKey{})
	})
}

func TestHandleLocal(t *testing.T) {
	grant := prefab.NewGrantInfo(t, "g1", "t/s/d", Global, "", "0", "a", LoadedGrantState)
	key := prefab.NewQDK(t, "t/s/d", "", "1")
	req := prefab.NewQDK(t, "t/s/d", "", "2")
	resp := prefab.NewQDK(t, "t/s/d", "", "3")

	ctx := context.Background()
	r := newFakeRange()

	t.Run("found and successful", func(t *testing.T) {
		resolver := newTestGrantResolver()
		resolver.grants.Activate(grant.ID(), grant.Shard(), r)

		rt, err := HandleLocal(ctx, resolver, key, req, localHandler(resp))
		requirex.Equal(t, rt, resp)
		require.NoError(t, err)
	})

	t.Run("found and fails", func(t *testing.T) {
		resolver := newTestGrantResolver()
		resolver.grants.Activate(grant.ID(), grant.Shard(), r)

		rt, err := HandleLocal(ctx, resolver, key, req, errHandler)
		requirex.Equal(t, rt, QualifiedDomainKey{})
		requirex.Equal(t, err, ErrInvalid)
	})

	t.Run("not found", func(t *testing.T) {
		resolver := newTestGrantResolver()

		rt, err := HandleLocal(ctx, resolver, key, req, localHandler(resp))
		requirex.Equal(t, rt, QualifiedDomainKey{})
		requirex.Equal(t, err, ToGRPCError(ErrNotOwned))
	})
}

func TestHandleLocalEx(t *testing.T) {
	grant := prefab.NewGrantInfo(t, "g1", "t/s/d", Global, "", "0", "a", LoadedGrantState)
	key := prefab.NewQDK(t, "t/s/d", "", "1")
	req := prefab.NewQDK(t, "t/s/d", "", "2")
	resp := prefab.NewQDK(t, "t/s/d", "", "3")

	ctx := context.Background()
	r := newFakeRange()

	t.Run("found and successful", func(t *testing.T) {
		resolver := newTestGrantResolver()
		resolver.grants.Activate(grant.ID(), grant.Shard(), r)

		rt, err := HandleLocalEx(ctx, resolver, key, req, ToGRPCError, localHandler(resp))
		requirex.Equal(t, rt, resp)
		require.NoError(t, err)
	})

	t.Run("found and fails", func(t *testing.T) {
		resolver := newTestGrantResolver()
		resolver.grants.Activate(grant.ID(), grant.Shard(), r)

		rt, err := HandleLocalEx(ctx, resolver, key, req, ToGRPCError, errHandler)
		requirex.Equal(t, rt, QualifiedDomainKey{})
		requirex.Equal(t, err, ToGRPCError(ErrInvalid))
	})

	t.Run("not found", func(t *testing.T) {
		resolver := newTestGrantResolver()

		rt, err := HandleLocalEx(ctx, resolver, key, req, ToGRPCError, localHandler(resp))
		requirex.Equal(t, rt, QualifiedDomainKey{})
		requirex.Equal(t, err, ToGRPCError(ErrNotOwned))
	})
}

type testGrantResolver struct {
	grants *GrantMap[*fakeRange]
}

func newTestGrantResolver() *testGrantResolver {
	return &testGrantResolver{
		grants: NewGrantMap[*fakeRange](),
	}
}

func (r *testGrantResolver) Lookup(key QualifiedDomainKey, grants ...GrantState) (*fakeRange, bool) {
	return r.grants.Lookup(key, grants...)
}

func (r *testGrantResolver) DomainKey(key QualifiedDomainKey) QualifiedDomainKey {
	return key
}

func (r *testGrantResolver) Location(key QualifiedDomainKey) (location.Location, bool) {
	return location.Location{}, false
}

func localHandler(resp QualifiedDomainKey) func(r *fakeRange, ctx context.Context, key QualifiedDomainKey) (QualifiedDomainKey, error) {
	return func(r *fakeRange, ctx context.Context, key QualifiedDomainKey) (QualifiedDomainKey, error) {
		return resp, nil
	}
}

func errHandler(r *fakeRange, ctx context.Context, key QualifiedDomainKey) (QualifiedDomainKey, error) {
	return QualifiedDomainKey{}, ErrInvalid
}

func remoteSuccess(resp QualifiedDomainKey) func(r int, ctx context.Context, key QualifiedDomainKey, opts ...grpc.CallOption) (QualifiedDomainKey, error) {
	return func(r int, ctx context.Context, key QualifiedDomainKey, opts ...grpc.CallOption) (QualifiedDomainKey, error) {
		return resp, nil
	}
}

func remoteInvalid(r int, ctx context.Context, key QualifiedDomainKey, opts ...grpc.CallOption) (QualifiedDomainKey, error) {
	return QualifiedDomainKey{}, ToGRPCError(ErrInvalid)
}

func localSuccess(resp QualifiedDomainKey) func(r *fakeRange) (QualifiedDomainKey, error) {
	return func(r *fakeRange) (QualifiedDomainKey, error) {
		return resp, nil
	}
}

func localFailure(r *fakeRange) (QualifiedDomainKey, error) {
	return QualifiedDomainKey{}, ErrInvalid
}

type testProxy struct {
	*testGrantResolver
	Resolver[int, QualifiedDomainKey]

	pool *fakePool
}

func newTestProxy() *testProxy {
	grantResolver := newTestGrantResolver()
	pool := newFakePool()
	resolver := NewResolver(pool, func(connInterface grpc.ClientConnInterface) int { return 0 })

	return &testProxy{
		testGrantResolver: grantResolver,
		Resolver:          resolver,

		pool: pool,
	}
}

func (r *testProxy) Cluster() (Cluster, bool) {
	return r.pool.Cluster()
}

func (r *testProxy) DomainKey(key QualifiedDomainKey) QualifiedDomainKey {
	return key
}

func (r *testProxy) Location(key QualifiedDomainKey) (location.Location, bool) {
	return r.Resolver.Location(key)
}
