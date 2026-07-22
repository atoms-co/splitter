package model

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"go.atoms.co/iox"
	"go.atoms.co/lib/chanx"
	"go.atoms.co/lib/net/grpcx"
	"go.atoms.co/lib/testing/assertx"
	"go.atoms.co/lib/testing/requirex"
	"go.atoms.co/lib/testing/synctestx"
	"go.atoms.co/splitter/lib/service/location"
)

var (
	service1 = QualifiedServiceName{Tenant: "tenant1", Service: "service1"}
	domain1  = QualifiedDomainName{Service: service1, Domain: "domain1"}
)

func newTestWorkPool(consumer Consumer, service QualifiedServiceName, domains []QualifiedDomainName, joinFn workPoolJoinFn, handlerFn Handler, poolOpts *workPoolOptions) (*workPool, <-chan Cluster) {
	return newWorkPool(consumer, service, domains, joinFn, handlerFn, poolOpts, NewOptions())
}

func TestWorkpool(t *testing.T) {
	// (1) Workpool sends REGISTER

	synctestx.Run(t, "register", func(t *testing.T) {
		coordinatorCon := newFakeCon[ConsumerMessage]()
		defer coordinatorCon.Close()

		shards := make(chan Shard)

		consumer := NewInstance(location.NewInstance(location.New("centralus", "pod1")), "endpoint")
		w, _ := newTestWorkPool(consumer, service1, []QualifiedDomainName{domain1},
			func(ctx context.Context, self location.Instance, handler grpcx.Handler[ConsumerMessage, ConsumerMessage]) error {
				return coordinatorCon.connect(ctx, handler)
			},
			func(ctx context.Context, id GrantID, shard Shard, ownership Ownership) {
				shards <- shard
				for {
					select {
					case <-ownership.Revoked().Closed():
						return
					case <-ctx.Done():
						return
					}
				}
			},
			&workPoolOptions{drainTimeout: time.Second},
		)
		defer func() {
			w.Drain()
			<-w.Closed()
		}()

		<-coordinatorCon.Connected.Closed()

		msg := assertx.Element(t, coordinatorCon.In)

		// << register
		cMsg, ok := msg.ClientMessage()
		assert.True(t, ok)

		register, ok := cMsg.Register()
		assert.True(t, ok)
		assert.Len(t, register.Active(), 0)

		// >> state (omitted) and lease

		coordinatorCon.Out <- NewExtend(time.Now().Add(time.Minute))
	})

	// (2) Grant/Revoke

	synctestx.Run(t, "grant", func(t *testing.T) {
		coordinatorCon := newFakeCon[ConsumerMessage]()
		defer coordinatorCon.Close()

		shards := make(chan Shard)

		consumer := NewInstance(location.NewInstance(location.New("centralus", "pod1")), "endpoint")
		w, _ := newTestWorkPool(consumer, service1, []QualifiedDomainName{domain1},
			func(ctx context.Context, self location.Instance, handler grpcx.Handler[ConsumerMessage, ConsumerMessage]) error {
				return coordinatorCon.connect(ctx, handler)
			},
			func(ctx context.Context, id GrantID, shard Shard, ownership Ownership) {
				shards <- shard
				for {
					select {
					case <-ownership.Revoked().Closed():
						return
					case <-ctx.Done():
						return
					}
				}
			},
			&workPoolOptions{drainTimeout: time.Second},
		)
		defer func() {
			w.Drain()
			<-w.Closed()
		}()

		<-coordinatorCon.Connected.Closed()

		assertx.Element(t, coordinatorCon.In)                        // register
		coordinatorCon.Out <- NewExtend(time.Now().Add(time.Minute)) // initial extend

		// << grant from coordinator --> coordinator creation

		shard := Shard{Domain: domain1, Type: Unit}
		grant := NewGrant("grant1", shard, ActiveGrantState, time.Now().Add(time.Minute), time.Now())
		coordinatorCon.Out <- NewAssign(grant)

		s := assertx.Element(t, shards)
		assert.Equal(t, shard, s)

		// << revoke grant1 from coordinator --> relinquish

		coordinatorCon.Out <- NewRevoke(grant)

		// >> relinquished

		msg := assertx.Element(t, coordinatorCon.In)
		cMsg, ok := msg.ClientMessage()
		assert.True(t, ok)
		assert.True(t, cMsg.IsReleased())
	})

	// (3) Workpool disconnect and reconnect to Leader

	synctestx.Run(t, "workpool/disconnect", func(t *testing.T) {
		coordinatorCon := newFakeCon[ConsumerMessage]()
		defer coordinatorCon.Close()

		shards := make(chan Shard)

		consumer := NewInstance(location.NewInstance(location.New("centralus", "pod1")), "endpoint")
		w, _ := newTestWorkPool(consumer, service1, []QualifiedDomainName{domain1},
			func(ctx context.Context, self location.Instance, handler grpcx.Handler[ConsumerMessage, ConsumerMessage]) error {
				return coordinatorCon.connect(ctx, handler)
			},
			func(ctx context.Context, id GrantID, shard Shard, ownership Ownership) {
				shards <- shard
				for {
					select {
					case <-ownership.Revoked().Closed():
						return
					case <-ctx.Done():
						return
					}
				}
			},
			&workPoolOptions{drainTimeout: time.Second},
		)
		defer func() {
			w.Drain()
			<-w.Closed()
		}()

		<-coordinatorCon.Connected.Closed()

		assertx.Element(t, coordinatorCon.In)                        // register
		coordinatorCon.Out <- NewExtend(time.Now().Add(time.Minute)) // initial extend

		// grant
		shard := Shard{Domain: domain1, Type: Unit}
		grant := NewGrant("grant1", shard, ActiveGrantState, time.Now().Add(time.Minute), time.Now())
		coordinatorCon.Out <- NewAssign(grant)
		assertx.Element(t, shards)

		// Shut down coordinator connection to force reconnect
		oldCoordinatorCon := coordinatorCon
		coordinatorCon = newFakeCon[ConsumerMessage]()
		defer coordinatorCon.Close()
		oldCoordinatorCon.Close()

		time.Sleep(2 * time.Second) // Random backoff

		<-coordinatorCon.Connected.Closed()

		msg := assertx.Element(t, coordinatorCon.In)
		cMsg, ok := msg.ClientMessage()
		assert.True(t, ok)
		register, ok := cMsg.Register()
		assert.True(t, ok)
		assert.Len(t, register.Active(), 1)
		assert.Equal(t, register.Active()[0].State(), ActiveGrantState)

		coordinatorCon.Out <- NewRevoke(grant)
	})

	synctestx.Run(t, "workpool/promotion", func(t *testing.T) {
		coordinatorCon := newFakeCon[ConsumerMessage]()
		defer coordinatorCon.Close()

		shards := make(chan Shard)

		consumer := NewInstance(location.NewInstance(location.New("centralus", "pod1")), "endpoint")
		w, _ := newTestWorkPool(consumer, service1, []QualifiedDomainName{domain1},
			func(ctx context.Context, self location.Instance, handler grpcx.Handler[ConsumerMessage, ConsumerMessage]) error {
				return coordinatorCon.connect(ctx, handler)
			},
			func(ctx context.Context, id GrantID, shard Shard, ownership Ownership) {
				shards <- shard
				for {
					select {
					case <-ctx.Done():
						return
					}
				}
			},
			&workPoolOptions{drainTimeout: time.Second},
		)
		defer func() {
			w.Drain()
			<-w.Closed()
		}()

		<-coordinatorCon.Connected.Closed()

		assertx.Element(t, coordinatorCon.In)                        // register
		coordinatorCon.Out <- NewExtend(time.Now().Add(time.Minute)) // initial extend

		// grant + promotion
		shard := Shard{Domain: domain1, Type: Unit}
		grant := NewGrant("grant1", shard, AllocatedGrantState, time.Now().Add(time.Minute), time.Now())
		coordinatorCon.Out <- NewAssign(grant)
		assertx.Element(t, shards)

		promotion := NewGrant("grant1", shard, ActiveGrantState, time.Now().Add(time.Minute), time.Now())
		coordinatorCon.Out <- NewPromote(promotion)
		assertx.NoElement(t, shards)

		// Shut down coordinator connection to force reconnect
		oldCoordinatorCon := coordinatorCon
		coordinatorCon = newFakeCon[ConsumerMessage]()
		defer coordinatorCon.Close()
		oldCoordinatorCon.Close()

		time.Sleep(2 * time.Second) // Random backoff

		<-coordinatorCon.Connected.Closed()

		msg := assertx.Element(t, coordinatorCon.In)
		cMsg, ok := msg.ClientMessage()
		assert.True(t, ok)
		register, ok := cMsg.Register()
		assert.True(t, ok)
		assert.Len(t, register.Active(), 1)
		assert.Equal(t, register.Active()[0].State(), ActiveGrantState)

		coordinatorCon.Out <- NewRevoke(grant)
	})

	synctestx.Run(t, "reconnect with promotion", func(t *testing.T) {
		coordinatorCon := newFakeCon[ConsumerMessage]()
		defer coordinatorCon.Close()

		shards := make(chan Shard)
		active := make(chan struct{}, 10)

		consumer := NewInstance(location.NewInstance(location.New("centralus", "pod1")), "endpoint")
		w, _ := newTestWorkPool(consumer, service1, []QualifiedDomainName{domain1},
			func(ctx context.Context, self location.Instance, handler grpcx.Handler[ConsumerMessage, ConsumerMessage]) error {
				return coordinatorCon.connect(ctx, handler)
			},
			func(ctx context.Context, id GrantID, shard Shard, ownership Ownership) {
				shards <- shard
				select {
				case <-ownership.Active().Closed():
					active <- struct{}{}
				case <-ctx.Done():
					return
				}

				select {
				case <-ownership.Revoked().Closed():
					return
				case <-ctx.Done():
					return
				}
			},
			&workPoolOptions{drainTimeout: time.Second},
		)
		defer func() {
			w.Drain()
			<-w.Closed()
		}()

		<-coordinatorCon.Connected.Closed()

		assertx.Element(t, coordinatorCon.In)                        // register
		coordinatorCon.Out <- NewExtend(time.Now().Add(time.Minute)) // initial extend

		// grant allocated
		shard := Shard{Domain: domain1, Type: Unit}
		grant := NewGrant("grant1", shard, AllocatedGrantState, time.Now().Add(time.Minute), time.Now())
		coordinatorCon.Out <- NewAssign(grant)
		assertx.Element(t, shards)
		requirex.ChanEmpty(t, active)

		// Shut down coordinator connection to force reconnect
		oldCoordinatorCon := coordinatorCon
		coordinatorCon = newFakeCon[ConsumerMessage]()
		defer coordinatorCon.Close()
		oldCoordinatorCon.Close()

		time.Sleep(2 * time.Second)

		<-coordinatorCon.Connected.Closed()

		msg := assertx.Element(t, coordinatorCon.In)
		cMsg, ok := msg.ClientMessage()
		assert.True(t, ok)
		register, ok := cMsg.Register()
		assert.True(t, ok)
		assert.Len(t, register.Active(), 1)
		assert.Equal(t, register.Active()[0].State(), AllocatedGrantState)

		coordinatorCon.Out <- NewExtend(time.Now().Add(time.Minute))

		// Send the same grant, but active
		grant = NewGrant("grant1", shard, ActiveGrantState, time.Now().Add(time.Minute), time.Now())
		coordinatorCon.Out <- NewAssign(grant)
		requirex.ChanEmpty(t, shards)
		// Grant is promoted to active
		assertx.Element(t, active)

		coordinatorCon.Out <- NewRevoke(grant)
	})

	synctestx.Run(t, "reconnect with loading", func(t *testing.T) {
		coordinatorCon := newFakeCon[ConsumerMessage]()
		defer coordinatorCon.Close()

		shards := make(chan Shard)
		revoked := make(chan struct{}, 10)
		load := iox.NewAsyncCloser()
		quit := iox.NewAsyncCloser()

		consumer := NewInstance(location.NewInstance(location.New("centralus", "pod1")), "endpoint")
		w, _ := newTestWorkPool(consumer, service1, []QualifiedDomainName{domain1},
			func(ctx context.Context, self location.Instance, handler grpcx.Handler[ConsumerMessage, ConsumerMessage]) error {
				return coordinatorCon.connect(ctx, handler)
			},
			func(ctx context.Context, id GrantID, shard Shard, ownership Ownership) {
				shards <- shard
				<-load.Closed()
				ownership.Loader().Load()
				select {
				case <-ownership.Expired().Closed():
					quit.Close()
				case <-ctx.Done():
					quit.Close()
				}
			},
			&workPoolOptions{drainTimeout: time.Second},
		)
		defer func() {
			w.Drain()
			<-w.Closed()
		}()

		<-coordinatorCon.Connected.Closed()

		assertx.Element(t, coordinatorCon.In)                        // register
		coordinatorCon.Out <- NewExtend(time.Now().Add(time.Minute)) // initial extend

		// grant allocated
		shard := Shard{Domain: domain1, Type: Unit}
		grant := NewGrant("grant1", shard, AllocatedGrantState, time.Now().Add(time.Minute), time.Now())
		coordinatorCon.Out <- NewAssign(grant)
		assertx.Element(t, shards)
		requirex.ChanEmpty(t, revoked)

		// Shut down coordinator connection to force reconnect
		oldCoordinatorCon := coordinatorCon
		coordinatorCon = newFakeCon[ConsumerMessage]()
		defer coordinatorCon.Close()
		oldCoordinatorCon.Close()

		time.Sleep(2 * time.Second)

		<-coordinatorCon.Connected.Closed()

		msg := assertx.Element(t, coordinatorCon.In)
		cMsg, ok := msg.ClientMessage()
		assert.True(t, ok)
		register, ok := cMsg.Register()
		assert.True(t, ok)
		assert.Len(t, register.Active(), 1)
		assert.Equal(t, register.Active()[0].State(), AllocatedGrantState)

		load.Close()

		coordinatorCon.Out <- NewExtend(time.Now().Add(time.Minute))

		// Send the same grant, but active
		grant = NewGrant("grant1", shard, AllocatedGrantState, time.Now().Add(time.Minute), time.Now())
		coordinatorCon.Out <- NewAssign(grant)
		requirex.ChanEmpty(t, shards)
		// Grant is not closed
		chanx.TryDrain(quit.Closed(), 100*time.Millisecond)

		coordinatorCon.Out <- NewRevoke(grant)
	})

	synctestx.Run(t, "handler context is cancelled when workpool is closed", func(t *testing.T) {
		coordinatorCon := newFakeCon[ConsumerMessage]()
		defer coordinatorCon.Close()

		start := iox.NewAsyncCloser()
		done := iox.NewAsyncCloser()

		consumer := NewInstance(location.NewInstance(location.New("centralus", "pod1")), "endpoint")
		w, _ := newTestWorkPool(consumer, service1, []QualifiedDomainName{domain1},
			func(ctx context.Context, self location.Instance, handler grpcx.Handler[ConsumerMessage, ConsumerMessage]) error {
				return coordinatorCon.connect(ctx, handler)
			},
			func(ctx context.Context, id GrantID, shard Shard, ownership Ownership) {
				start.Close()
				<-ctx.Done()
				done.Close()
			},
			&workPoolOptions{drainTimeout: time.Second},
		)
		defer func() {
			w.Drain()
			<-w.Closed()
		}()

		<-coordinatorCon.Connected.Closed()

		requirex.Element(t, coordinatorCon.In)                       // register
		coordinatorCon.Out <- NewExtend(time.Now().Add(time.Minute)) // initial extend

		shard := Shard{Domain: domain1, Type: Unit}
		grant := NewGrant("grant1", shard, ActiveGrantState, time.Now().Add(time.Minute), time.Now())
		coordinatorCon.Out <- NewAssign(grant)

		requirex.Closed(t, start.Closed())
		w.Drain()
		time.Sleep(2 * time.Second)

		requirex.Closed(t, done.Closed())
	})

	synctestx.Run(t, "grant is revoked when workpool is disconnected after drain", func(t *testing.T) {
		coordinatorCon := newFakeCon[ConsumerMessage]()
		defer coordinatorCon.Close()

		start := iox.NewAsyncCloser()
		done := iox.NewAsyncCloser()

		consumer := NewInstance(location.NewInstance(location.New("centralus", "pod1")), "endpoint")
		w, _ := newTestWorkPool(consumer, service1, []QualifiedDomainName{domain1},
			func(ctx context.Context, self location.Instance, handler grpcx.Handler[ConsumerMessage, ConsumerMessage]) error {
				return coordinatorCon.connect(ctx, handler)
			},
			func(ctx context.Context, id GrantID, shard Shard, ownership Ownership) {
				start.Close()
				<-ownership.Revoked().Closed()
				done.Close()
			},
			&workPoolOptions{drainTimeout: time.Second},
		)
		defer func() {
			w.Drain()
			<-w.Closed()
		}()

		<-coordinatorCon.Connected.Closed()

		requirex.Element(t, coordinatorCon.In)                       // register
		coordinatorCon.Out <- NewExtend(time.Now().Add(time.Minute)) // initial extend

		shard := Shard{Domain: domain1, Type: Unit}
		grant := NewGrant("grant1", shard, ActiveGrantState, time.Now().Add(time.Minute), time.Now())
		coordinatorCon.Out <- NewAssign(grant)

		requirex.Closed(t, start.Closed())

		w.Drain()

		// Shut down coordinator connection to force reconnect
		oldCoordinatorCon := coordinatorCon
		coordinatorCon = newFakeCon[ConsumerMessage]()
		defer coordinatorCon.Close()
		oldCoordinatorCon.Close()

		requirex.Closed(t, done.Closed())
	})

	synctestx.Run(t, "drains after disconnected timeout", func(t *testing.T) {
		connections := make(chan *fakeCon[ConsumerMessage], 1)
		coordinatorCon := newFakeCon[ConsumerMessage]()
		connections <- coordinatorCon

		started := iox.NewAsyncCloser()
		revoked := iox.NewAsyncCloser()
		consumer := NewInstance(location.NewInstance(location.New("centralus", "pod1")), "endpoint")
		w, _ := newTestWorkPool(consumer, service1, []QualifiedDomainName{domain1},
			func(ctx context.Context, self location.Instance, handler grpcx.Handler[ConsumerMessage, ConsumerMessage]) error {
				select {
				case con := <-connections:
					return con.connect(ctx, handler)
				case <-ctx.Done():
					return ctx.Err()
				}
			},
			func(ctx context.Context, id GrantID, shard Shard, ownership Ownership) {
				started.Close()
				select {
				case <-ownership.Revoked().Closed():
					revoked.Close()
				case <-ctx.Done():
				}
			},
			&workPoolOptions{disconnectTimeout: time.Second, drainTimeout: 2 * time.Second},
		)
		defer func() {
			w.Drain()
			<-w.Closed()
		}()

		requirex.Closed(t, coordinatorCon.Connected.Closed())
		requirex.Element(t, coordinatorCon.In)                       // register
		coordinatorCon.Out <- NewExtend(time.Now().Add(time.Minute)) // initial extend
		shard := Shard{Domain: domain1, Type: Unit}
		grant := NewGrant("grant1", shard, ActiveGrantState, time.Now().Add(time.Minute), time.Now())
		coordinatorCon.Out <- NewAssign(grant)
		requirex.Closed(t, started.Closed())

		coordinatorCon.Close()
		time.Sleep(2 * time.Second)

		requirex.Closed(t, revoked.Closed())
		requirex.Closed(t, w.Closed())
	})

	synctestx.Run(t, "reconnect cancels timeout", func(t *testing.T) {
		connections := make(chan *fakeCon[ConsumerMessage], 2)
		firstCon := newFakeCon[ConsumerMessage]()
		secondCon := newFakeCon[ConsumerMessage]()
		connections <- firstCon

		consumer := NewInstance(location.NewInstance(location.New("centralus", "pod1")), "endpoint")
		w, _ := newTestWorkPool(consumer, service1, []QualifiedDomainName{domain1},
			func(ctx context.Context, self location.Instance, handler grpcx.Handler[ConsumerMessage, ConsumerMessage]) error {
				select {
				case con := <-connections:
					return con.connect(ctx, handler)
				case <-ctx.Done():
					return ctx.Err()
				}
			},
			func(ctx context.Context, id GrantID, shard Shard, ownership Ownership) {
				<-ctx.Done()
			},
			&workPoolOptions{disconnectTimeout: 10 * time.Second, drainTimeout: 2 * time.Second},
		)
		defer func() {
			w.Drain()
			<-w.Closed()
		}()

		requirex.Closed(t, firstCon.Connected.Closed())
		requirex.Element(t, firstCon.In) // register
		firstCon.Close()

		time.Sleep(2 * time.Second)
		connections <- secondCon
		requirex.Closed(t, secondCon.Connected.Closed())
		requirex.Element(t, secondCon.In) // register

		time.Sleep(6 * time.Second)
		select {
		case <-w.Closed():
			t.Fatal("work pool closed after reconnect")
		default:
		}
	})

	synctestx.Run(t, "release grant", func(t *testing.T) {
		coordinatorCon := newFakeCon[ConsumerMessage]()
		defer coordinatorCon.Close()

		done := iox.NewAsyncCloser()

		consumer := NewInstance(location.NewInstance(location.New("centralus", "pod1")), "endpoint")
		w, _ := newTestWorkPool(consumer, service1, []QualifiedDomainName{domain1},
			func(ctx context.Context, self location.Instance, handler grpcx.Handler[ConsumerMessage, ConsumerMessage]) error {
				return coordinatorCon.connect(ctx, handler)
			},
			func(ctx context.Context, id GrantID, shard Shard, ownership Ownership) {
				ownership.RequestRevoke() // request revoke grant
				select {
				case <-done.Closed():
					return
				}
			},
			&workPoolOptions{drainTimeout: time.Second},
		)
		defer func() {
			w.Drain()
			<-w.Closed()
		}()

		<-coordinatorCon.Connected.Closed()

		requirex.Element(t, coordinatorCon.In)                       // register
		coordinatorCon.Out <- NewExtend(time.Now().Add(time.Minute)) // initial extend

		shard := Shard{Domain: domain1, Type: Unit}
		grant := NewGrant("grant1", shard, ActiveGrantState, time.Now().Add(time.Minute), time.Now())
		coordinatorCon.Out <- NewAssign(grant)

		cMsg := requirex.Element(t, coordinatorCon.In)
		clientMsg, ok := cMsg.ClientMessage()
		assert.True(t, ok)
		revokeMsg, ok := clientMsg.Revoke()
		assert.True(t, ok)
		assert.Len(t, revokeMsg.Grants(), 1)
		assert.Equal(t, revokeMsg.Grants()[0], grant)

		done.Close()
	})

}

type fakeCon[T any] struct {
	iox.AsyncCloser
	Connected iox.AsyncCloser
	In        <-chan T
	Out       chan<- T
}

func newFakeCon[T any]() *fakeCon[T] {
	return &fakeCon[T]{
		AsyncCloser: iox.NewAsyncCloser(),
		Connected:   iox.NewAsyncCloser(),
	}
}

func (f *fakeCon[T]) connect(ctx context.Context, handler grpcx.Handler[T, T]) error {
	in := make(chan T, 20)
	defer close(in)

	out, err := handler(ctx, in)
	if err != nil {
		return err
	}
	f.In = out
	f.Out = in
	f.Connected.Close()

	select {
	case <-f.Closed():
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
