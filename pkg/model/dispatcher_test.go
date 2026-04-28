package model_test

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc"

	"go.atoms.co/splitter/lib/service/location"
	"go.atoms.co/lib/testing/assertx"
	"go.atoms.co/iox"
	"go.atoms.co/splitter/pkg/model"
)

func TestDispatcher(t *testing.T) {
	ctx := context.Background()

	loc := location.New("us", "n1")
	endpoint := "endpoint"

	service := model.MustParseQualifiedServiceNameStr("tenant/service")

	t.Run("join", func(t *testing.T) {
		wctx, cancel := context.WithCancel(ctx)

		client := newFakeClient()

		// (1) Join passes information correctly.

		dispatcher := model.NewDispatcher(wctx, client, loc, endpoint, service, nil)
		assertx.Equal(t, client.Service, service)
		assertx.Equal(t, client.Consumer.Location(), loc)
		assertx.Equal(t, client.Consumer.Endpoint(), endpoint)

		assertx.Equal(t, client.IsClosed(), false)
		assertx.Equal(t, client.Drained.IsClosed(), false)
		assertx.Equal(t, dispatcher.IsClosed(), false)

		// (2) Context cancellation drains. Closed == client.Closed.

		cancel()

		assertx.Closed(t, client.Drained.Closed())
		assertx.Equal(t, client.IsClosed(), false)
		assertx.Equal(t, dispatcher.IsClosed(), false)

		client.Close()

		assertx.Closed(t, dispatcher.Closed())
	})

	t.Run("filter", func(t *testing.T) {
		client := newFakeClient()
		defer client.Close()

		var counter int

		dispatcher := model.NewDispatcher(ctx, client, loc, endpoint, service, []model.DispatchFilter{
			filter(func(shard model.Shard) bool {
				counter += 1
				return shard.Domain.Domain == "a"
			}),
			filter(func(shard model.Shard) bool {
				if shard.Domain.Domain == "a" {
					t.Fatalf("selected shadowed filter")
					return true // never selected
				}
				return false
			}),
			filter(func(shard model.Shard) bool {
				counter += 10
				return shard.Domain.Domain == "b"
			}),
			filter(func(shard model.Shard) bool {
				counter += 100
				return true
			}),
		})
		defer dispatcher.Drain(time.Second)

		// (1) Check that chain works as expected. Use digits XYZ for concise comparisons of what is evaluated

		shard := model.Shard{Type: model.Unit, Domain: model.QualifiedDomainName{Service: service, Domain: "a"}}

		client.Handler(ctx, "1", shard, newTestOwnership())
		assertx.Equal(t, counter, 1)
		client.Handler(ctx, "1", shard, newTestOwnership())
		assertx.Equal(t, counter, 2)

		shard.Domain.Domain = "b"

		client.Handler(ctx, "1", shard, newTestOwnership())
		assertx.Equal(t, counter, 13)

		shard.Domain.Domain = "c"

		client.Handler(ctx, "1", shard, newTestOwnership())
		assertx.Equal(t, counter, 124)
	})
}

func TestProcessor(t *testing.T) {
	ctx := context.Background()

	remoteFn := func(connInterface grpc.ClientConnInterface) int { return 0 }

	domain := model.MustParseQualifiedDomainNameStr("tenant/service/domain")
	key := model.ZeroKey

	t.Run("handle", func(t *testing.T) {
		pool := newFakePool()

		created := iox.NewAsyncCloser()
		r := newFakeRange()

		proc := model.NewProcessor(domain.Domain, remoteFn, model.ToDomainKey[model.Key], func(ctx context.Context, grant model.GrantID, shard model.Shard, ownership model.Ownership) *fakeRange {
			defer created.Close()
			return r
		})
		proc.Init(domain.Service, pool)

		// (1) Handle is blocking, so do it async.

		lease := newTestOwnership()

		returned := iox.NewAsyncCloser()
		go func() {
			defer returned.Close()

			shard := model.Shard{Type: model.Global, Domain: domain, From: model.ZeroKey, To: model.MaxKey}
			ok := proc.TryHandle(ctx, "1", shard, lease)
			assertx.Equal(t, ok, true)
		}()

		time.Sleep(50 * time.Millisecond)

		assertx.Equal(t, created.IsClosed(), false)
		assertx.Equal(t, returned.IsClosed(), false)

		// (2) ALLOCATED: Wait for counterpart to unload. Range is not created until then.

		lease.loader.unloaded.Close()

		assertx.Closed(t, created.Closed())
		assertx.Equal(t, returned.IsClosed(), false)

		// (3) LOADED: The range is not owner until Loaded and do not show up in Lookup until then

		_, ok := proc.Lookup(key)
		assertx.Equal(t, ok, false)
		assertx.Equal(t, lease.loader.load.IsClosed(), false)

		r.Init.Close()

		assertx.Closed(t, lease.loader.load.Closed())

		r2, ok := proc.Lookup(key)
		assertx.Equal(t, ok, true)
		assertx.Equal(t, r2, r)

		// (4) ACTIVE: When activated, there is no change to the range.

		lease.active.Close()

		assertx.Equal(t, r.Draining.IsClosed(), false)
		assertx.Equal(t, r.Unloaded.IsClosed(), false)

		// (5) REVOKED: Revoke signals draining. Waits for unload.

		lease.revoked.Close()

		assertx.Closed(t, r.Draining.Closed())
		assertx.Equal(t, lease.unloader.unload.IsClosed(), false)

		// (5) UNLOADED: Once drained, wait for counterpart to load.

		r.Unloaded.Close()

		assertx.Closed(t, lease.unloader.unload.Closed())
		assertx.Equal(t, lease.unloader.loaded.IsClosed(), false)

		// (6) RELINQUISHED. Once counterpart has loaded, exit.

		lease.unloader.loaded.Close()

		assertx.Closed(t, returned.Closed())
		assertx.Closed(t, r.Closed())
	})

	t.Run("range_init_failure", func(t *testing.T) {
		pool := newFakePool()

		r := newFakeRange()
		proc := model.NewProcessor(domain.Domain, remoteFn, model.ToDomainKey[model.Key], func(ctx context.Context, grant model.GrantID, shard model.Shard, ownership model.Ownership) *fakeRange {
			return r
		})
		proc.Init(domain.Service, pool)

		// (1) Progress range to UNLOADED.

		lease := newTestOwnership()

		returned := iox.NewAsyncCloser()
		go func() {
			defer returned.Close()

			shard := model.Shard{Type: model.Global, Domain: domain, From: model.ZeroKey, To: model.MaxKey}
			ok := proc.TryHandle(ctx, "1", shard, lease)
			assertx.Equal(t, ok, true)
		}()

		lease.loader.unloaded.Close()

		// (2) Close range unprovoked w/o closing init. Forces immediate exit

		r.Close()

		assertx.Closed(t, returned.Closed())

		_, ok := proc.Lookup(key)
		assertx.Equal(t, ok, false)
	})

	t.Run("range_expire", func(t *testing.T) {
		pool := newFakePool()

		r := newFakeRange()
		proc := model.NewProcessor(domain.Domain, remoteFn, model.ToDomainKey[model.Key], func(ctx context.Context, grant model.GrantID, shard model.Shard, ownership model.Ownership) *fakeRange {
			return r
		})
		proc.Init(domain.Service, pool)

		// (1) Progress range to ACTIVE.

		lease := newTestOwnership()

		returned := iox.NewAsyncCloser()
		go func() {
			defer returned.Close()

			shard := model.Shard{Type: model.Global, Domain: domain, From: model.ZeroKey, To: model.MaxKey}
			ok := proc.TryHandle(ctx, "1", shard, lease)
			assertx.Equal(t, ok, true)
		}()

		lease.loader.unloaded.Close()
		r.Init.Close()
		assertx.Closed(t, lease.loader.load.Closed())

		lease.active.Close()

		assertx.Equal(t, r.Draining.IsClosed(), false)
		assertx.Equal(t, r.Unloaded.IsClosed(), false)

		// (2) Lease expiration, notably from lost connectivity. Forces immediate exit

		lease.expired.Close()

		assertx.Closed(t, returned.Closed())
		assertx.Closed(t, r.Closed())

		_, ok := proc.Lookup(key)
		assertx.Equal(t, ok, false)
	})
}

// fakeRange exposes manual control over lifecycle operations.
type fakeRange struct {
	iox.AsyncCloser
	Init, Draining, Unloaded iox.AsyncCloser
}

func newFakeRange() *fakeRange {
	return &fakeRange{
		AsyncCloser: iox.NewAsyncCloser(),
		Init:        iox.NewAsyncCloser(),
		Draining:    iox.NewAsyncCloser(),
		Unloaded:    iox.NewAsyncCloser(),
	}
}

func (f *fakeRange) Initialized() iox.RAsyncCloser {
	return f.Init
}

func (f *fakeRange) Drain(ctx context.Context, timeout time.Duration) iox.RAsyncCloser {
	f.Draining.Close()
	return f.Unloaded
}

// fakePool is manual connection pool. Not thread safe.
type fakePool struct {
	Current    model.Cluster
	Resolution map[model.InstanceID]grpc.ClientConnInterface
	Failed     map[model.InstanceID]error
}

func newFakePool() *fakePool {
	return &fakePool{
		Resolution: map[model.InstanceID]grpc.ClientConnInterface{},
		Failed:     map[model.InstanceID]error{},
	}
}

func (f *fakePool) Resolve(ctx context.Context, key model.Instance) (grpc.ClientConnInterface, error) {
	if err, ok := f.Failed[key.ID()]; ok {
		return nil, err
	}
	if cc, ok := f.Resolution[key.ID()]; ok {
		return cc, nil
	}
	return nil, model.ErrNoResolution
}

func (f *fakePool) Cluster() (model.Cluster, bool) {
	return f.Current, f.Current != nil
}

func (f *fakePool) Location(key model.QualifiedDomainKey) (location.Location, bool) {
	if c, ok := f.Cluster(); ok {
		if consumer, _, ok := c.Lookup(key); ok {
			return consumer.Location(), true
		}
	}
	return location.Location{}, false
}

// filter is a simple DispatchFilter for checking shards.
type filter func(shard model.Shard) bool

func (f filter) Init(service model.QualifiedServiceName, pool model.ConnectionPool) {

}

func (f filter) TryHandle(ctx context.Context, id model.GrantID, shard model.Shard, ownership model.Ownership) bool {
	return f(shard)
}

// fakeClient exposes join information and manual control. Fields set once joined. Not thread safe.
// Wait on Dispatcher creation.
type fakeClient struct {
	iox.AsyncCloser
	Drained iox.AsyncCloser

	Consumer model.Consumer
	Service  model.QualifiedServiceName
	Handler  model.Handler
	Clusters chan<- model.Cluster
}

func newFakeClient() *fakeClient {
	return &fakeClient{
		AsyncCloser: iox.NewAsyncCloser(),
	}
}

func (f *fakeClient) Join(ctx context.Context, consumer model.Consumer, service model.QualifiedServiceName, handler model.Handler, opts ...model.ConsumerOption) (<-chan model.Cluster, iox.RAsyncCloser) {
	ch := make(chan model.Cluster)

	f.Drained = iox.WithCancel(ctx, iox.NewAsyncCloser())
	f.Consumer = consumer
	f.Service = service
	f.Handler = handler
	f.Clusters = ch

	return ch, f
}
