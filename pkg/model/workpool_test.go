package model_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"atoms.co/lib-go/pkg/clock"
	"go.atoms.co/splitter/lib/service/location"
	"go.atoms.co/lib/testing/assertx"
	"go.atoms.co/lib/testing/requirex"
	"go.atoms.co/lib/testing/synctestx"
	"go.atoms.co/lib/chanx"
	"go.atoms.co/lib/net/grpcx"
	"go.atoms.co/lib/iox"
	"go.atoms.co/splitter/pkg/model"
)

var (
	service1 = model.QualifiedServiceName{Tenant: "tenant1", Service: "service1"}
	domain1  = model.QualifiedDomainName{Service: service1, Domain: "domain1"}
)

func TestWorkpool(t *testing.T) {
	// (1) Workpool sends REGISTER

	synctestx.Run(t, "register", func(t *testing.T) {
		coordinatorCon := newFakeCon[model.ConsumerMessage]()
		defer coordinatorCon.Close()

		shards := make(chan model.Shard)

		consumer := model.NewInstance(location.NewInstance(location.New("centralus", "pod1")), "endpoint")
		w, _ := model.NewWorkPool(clock.New(), consumer, service1, []model.QualifiedDomainName{domain1},
			func(ctx context.Context, self location.Instance, handler grpcx.Handler[model.ConsumerMessage, model.ConsumerMessage]) error {
				return coordinatorCon.connect(ctx, handler)
			},
			func(ctx context.Context, id model.GrantID, shard model.Shard, ownership model.Ownership) {
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
		)
		defer w.Drain(time.Second)
		<-coordinatorCon.Connected.Closed()

		msg := assertx.Element(t, coordinatorCon.In)

		// << register
		cMsg, ok := msg.ClientMessage()
		assert.True(t, ok)

		register, ok := cMsg.Register()
		assert.True(t, ok)
		assert.Len(t, register.Active(), 0)

		// >> state (omitted) and lease

		coordinatorCon.Out <- model.NewExtend(time.Now().Add(time.Minute))
	})

	// (2) Grant/Revoke

	synctestx.Run(t, "grant", func(t *testing.T) {
		coordinatorCon := newFakeCon[model.ConsumerMessage]()
		defer coordinatorCon.Close()

		shards := make(chan model.Shard)

		consumer := model.NewInstance(location.NewInstance(location.New("centralus", "pod1")), "endpoint")
		w, _ := model.NewWorkPool(clock.New(), consumer, service1, []model.QualifiedDomainName{domain1},
			func(ctx context.Context, self location.Instance, handler grpcx.Handler[model.ConsumerMessage, model.ConsumerMessage]) error {
				return coordinatorCon.connect(ctx, handler)
			},
			func(ctx context.Context, id model.GrantID, shard model.Shard, ownership model.Ownership) {
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
		)
		defer w.Drain(time.Second)
		<-coordinatorCon.Connected.Closed()

		assertx.Element(t, coordinatorCon.In)                              // register
		coordinatorCon.Out <- model.NewExtend(time.Now().Add(time.Minute)) // initial extend

		// << grant from coordinator --> coordinator creation

		shard := model.Shard{Domain: domain1, Type: model.Unit}
		grant := model.NewGrant("grant1", shard, model.ActiveGrantState, time.Now().Add(time.Minute), time.Now())
		coordinatorCon.Out <- model.NewAssign(grant)

		s := assertx.Element(t, shards)
		assert.Equal(t, shard, s)

		// << revoke grant1 from coordinator --> relinquish

		coordinatorCon.Out <- model.NewRevoke(grant)

		// >> relinquished

		msg := assertx.Element(t, coordinatorCon.In)
		cMsg, ok := msg.ClientMessage()
		assert.True(t, ok)
		assert.True(t, cMsg.IsReleased())
	})

	// (3) Workpool disconnect and reconnect to Leader

	synctestx.Run(t, "workpool/disconnect", func(t *testing.T) {
		coordinatorCon := newFakeCon[model.ConsumerMessage]()
		defer coordinatorCon.Close()

		shards := make(chan model.Shard)

		consumer := model.NewInstance(location.NewInstance(location.New("centralus", "pod1")), "endpoint")
		w, _ := model.NewWorkPool(clock.New(), consumer, service1, []model.QualifiedDomainName{domain1},
			func(ctx context.Context, self location.Instance, handler grpcx.Handler[model.ConsumerMessage, model.ConsumerMessage]) error {
				return coordinatorCon.connect(ctx, handler)
			},
			func(ctx context.Context, id model.GrantID, shard model.Shard, ownership model.Ownership) {
				shards <- shard
				for {
					select {
					case <-ctx.Done():
						return
					}
				}
			},
		)
		defer w.Drain(time.Second)
		<-coordinatorCon.Connected.Closed()

		assertx.Element(t, coordinatorCon.In)                              // register
		coordinatorCon.Out <- model.NewExtend(time.Now().Add(time.Minute)) // initial extend

		// grant
		shard := model.Shard{Domain: domain1, Type: model.Unit}
		grant := model.NewGrant("grant1", shard, model.ActiveGrantState, time.Now().Add(time.Minute), time.Now())
		coordinatorCon.Out <- model.NewAssign(grant)
		assertx.Element(t, shards)

		// Shut down coordinator connection to force reconnect
		oldCoordinatorCon := coordinatorCon
		coordinatorCon = newFakeCon[model.ConsumerMessage]()
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
		assert.Equal(t, register.Active()[0].State(), model.ActiveGrantState)

		coordinatorCon.Out <- model.NewRevoke(grant)
	})

	synctestx.Run(t, "workpool/promotion", func(t *testing.T) {
		coordinatorCon := newFakeCon[model.ConsumerMessage]()
		defer coordinatorCon.Close()

		shards := make(chan model.Shard)

		consumer := model.NewInstance(location.NewInstance(location.New("centralus", "pod1")), "endpoint")
		w, _ := model.NewWorkPool(clock.New(), consumer, service1, []model.QualifiedDomainName{domain1},
			func(ctx context.Context, self location.Instance, handler grpcx.Handler[model.ConsumerMessage, model.ConsumerMessage]) error {
				return coordinatorCon.connect(ctx, handler)
			},
			func(ctx context.Context, id model.GrantID, shard model.Shard, ownership model.Ownership) {
				shards <- shard
				for {
					select {
					case <-ctx.Done():
						return
					}
				}
			},
		)
		defer w.Drain(time.Second)
		<-coordinatorCon.Connected.Closed()

		assertx.Element(t, coordinatorCon.In)                              // register
		coordinatorCon.Out <- model.NewExtend(time.Now().Add(time.Minute)) // initial extend

		// grant + promotion
		shard := model.Shard{Domain: domain1, Type: model.Unit}
		grant := model.NewGrant("grant1", shard, model.AllocatedGrantState, time.Now().Add(time.Minute), time.Now())
		coordinatorCon.Out <- model.NewAssign(grant)
		assertx.Element(t, shards)

		promotion := model.NewGrant("grant1", shard, model.ActiveGrantState, time.Now().Add(time.Minute), time.Now())
		coordinatorCon.Out <- model.NewPromote(promotion)
		assertx.NoElement(t, shards)

		// Shut down coordinator connection to force reconnect
		oldCoordinatorCon := coordinatorCon
		coordinatorCon = newFakeCon[model.ConsumerMessage]()
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
		assert.Equal(t, register.Active()[0].State(), model.ActiveGrantState)

		coordinatorCon.Out <- model.NewRevoke(grant)
	})

	synctestx.Run(t, "reconnect with promotion", func(t *testing.T) {
		coordinatorCon := newFakeCon[model.ConsumerMessage]()
		defer coordinatorCon.Close()

		shards := make(chan model.Shard)
		active := make(chan struct{}, 10)

		consumer := model.NewInstance(location.NewInstance(location.New("centralus", "pod1")), "endpoint")
		w, _ := model.NewWorkPool(clock.New(), consumer, service1, []model.QualifiedDomainName{domain1},
			func(ctx context.Context, self location.Instance, handler grpcx.Handler[model.ConsumerMessage, model.ConsumerMessage]) error {
				return coordinatorCon.connect(ctx, handler)
			},
			func(ctx context.Context, id model.GrantID, shard model.Shard, ownership model.Ownership) {
				shards <- shard
				select {
				case <-ownership.Active().Closed():
					active <- struct{}{}
				case <-ctx.Done():
					return
				}

				select {
				case <-ctx.Done():
					return
				}
			},
		)
		defer w.Drain(time.Second)
		<-coordinatorCon.Connected.Closed()

		assertx.Element(t, coordinatorCon.In)                              // register
		coordinatorCon.Out <- model.NewExtend(time.Now().Add(time.Minute)) // initial extend

		// grant allocated
		shard := model.Shard{Domain: domain1, Type: model.Unit}
		grant := model.NewGrant("grant1", shard, model.AllocatedGrantState, time.Now().Add(time.Minute), time.Now())
		coordinatorCon.Out <- model.NewAssign(grant)
		assertx.Element(t, shards)
		requirex.ChanEmpty(t, active)

		// Shut down coordinator connection to force reconnect
		oldCoordinatorCon := coordinatorCon
		coordinatorCon = newFakeCon[model.ConsumerMessage]()
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
		assert.Equal(t, register.Active()[0].State(), model.AllocatedGrantState)

		coordinatorCon.Out <- model.NewExtend(time.Now().Add(time.Minute))

		// Send the same grant, but active
		grant = model.NewGrant("grant1", shard, model.ActiveGrantState, time.Now().Add(time.Minute), time.Now())
		coordinatorCon.Out <- model.NewAssign(grant)
		requirex.ChanEmpty(t, shards)
		// Grant is promoted to active
		assertx.Element(t, active)

		coordinatorCon.Out <- model.NewRevoke(grant)
	})

	synctestx.Run(t, "reconnect with loading", func(t *testing.T) {
		coordinatorCon := newFakeCon[model.ConsumerMessage]()
		defer coordinatorCon.Close()

		shards := make(chan model.Shard)
		revoked := make(chan struct{}, 10)
		load := iox.NewAsyncCloser()
		quit := iox.NewAsyncCloser()

		consumer := model.NewInstance(location.NewInstance(location.New("centralus", "pod1")), "endpoint")
		w, _ := model.NewWorkPool(clock.New(), consumer, service1, []model.QualifiedDomainName{domain1},
			func(ctx context.Context, self location.Instance, handler grpcx.Handler[model.ConsumerMessage, model.ConsumerMessage]) error {
				return coordinatorCon.connect(ctx, handler)
			},
			func(ctx context.Context, id model.GrantID, shard model.Shard, ownership model.Ownership) {
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
		)
		defer w.Drain(time.Second)
		<-coordinatorCon.Connected.Closed()

		assertx.Element(t, coordinatorCon.In)                              // register
		coordinatorCon.Out <- model.NewExtend(time.Now().Add(time.Minute)) // initial extend

		// grant allocated
		shard := model.Shard{Domain: domain1, Type: model.Unit}
		grant := model.NewGrant("grant1", shard, model.AllocatedGrantState, time.Now().Add(time.Minute), time.Now())
		coordinatorCon.Out <- model.NewAssign(grant)
		assertx.Element(t, shards)
		requirex.ChanEmpty(t, revoked)

		// Shut down coordinator connection to force reconnect
		oldCoordinatorCon := coordinatorCon
		coordinatorCon = newFakeCon[model.ConsumerMessage]()
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
		assert.Equal(t, register.Active()[0].State(), model.AllocatedGrantState)

		load.Close()

		coordinatorCon.Out <- model.NewExtend(time.Now().Add(time.Minute))

		// Send the same grant, but active
		grant = model.NewGrant("grant1", shard, model.AllocatedGrantState, time.Now().Add(time.Minute), time.Now())
		coordinatorCon.Out <- model.NewAssign(grant)
		requirex.ChanEmpty(t, shards)
		// Grant is not closed
		chanx.TryDrain(quit.Closed(), clock.New(), 100*time.Millisecond)

		coordinatorCon.Out <- model.NewRevoke(grant)
	})

	synctestx.Run(t, "handler context is cancelled when workpool is closed", func(t *testing.T) {
		coordinatorCon := newFakeCon[model.ConsumerMessage]()
		defer coordinatorCon.Close()

		start := iox.NewAsyncCloser()
		done := iox.NewAsyncCloser()

		consumer := model.NewInstance(location.NewInstance(location.New("centralus", "pod1")), "endpoint")
		w, _ := model.NewWorkPool(clock.New(), consumer, service1, []model.QualifiedDomainName{domain1},
			func(ctx context.Context, self location.Instance, handler grpcx.Handler[model.ConsumerMessage, model.ConsumerMessage]) error {
				return coordinatorCon.connect(ctx, handler)
			},
			func(ctx context.Context, id model.GrantID, shard model.Shard, ownership model.Ownership) {
				start.Close()
				<-ctx.Done()
				done.Close()
			},
		)
		defer w.Drain(time.Second)
		<-coordinatorCon.Connected.Closed()

		requirex.Element(t, coordinatorCon.In)                             // register
		coordinatorCon.Out <- model.NewExtend(time.Now().Add(time.Minute)) // initial extend

		shard := model.Shard{Domain: domain1, Type: model.Unit}
		grant := model.NewGrant("grant1", shard, model.ActiveGrantState, time.Now().Add(time.Minute), time.Now())
		coordinatorCon.Out <- model.NewAssign(grant)

		requirex.Closed(t, start.Closed())
		w.Drain(time.Second)
		time.Sleep(2 * time.Second)

		requirex.Closed(t, done.Closed())
	})

	synctestx.Run(t, "grant is revoked when workpool is disconnected after drain", func(t *testing.T) {
		coordinatorCon := newFakeCon[model.ConsumerMessage]()
		defer coordinatorCon.Close()

		start := iox.NewAsyncCloser()
		done := iox.NewAsyncCloser()

		consumer := model.NewInstance(location.NewInstance(location.New("centralus", "pod1")), "endpoint")
		w, _ := model.NewWorkPool(clock.New(), consumer, service1, []model.QualifiedDomainName{domain1},
			func(ctx context.Context, self location.Instance, handler grpcx.Handler[model.ConsumerMessage, model.ConsumerMessage]) error {
				return coordinatorCon.connect(ctx, handler)
			},
			func(ctx context.Context, id model.GrantID, shard model.Shard, ownership model.Ownership) {
				start.Close()
				<-ownership.Revoked().Closed()
				done.Close()
			},
		)
		defer w.Drain(time.Second)
		<-coordinatorCon.Connected.Closed()

		requirex.Element(t, coordinatorCon.In)                             // register
		coordinatorCon.Out <- model.NewExtend(time.Now().Add(time.Minute)) // initial extend

		shard := model.Shard{Domain: domain1, Type: model.Unit}
		grant := model.NewGrant("grant1", shard, model.ActiveGrantState, time.Now().Add(time.Minute), time.Now())
		coordinatorCon.Out <- model.NewAssign(grant)

		requirex.Closed(t, start.Closed())

		w.Drain(10 * time.Second)

		// Shut down coordinator connection to force reconnect
		oldCoordinatorCon := coordinatorCon
		coordinatorCon = newFakeCon[model.ConsumerMessage]()
		defer coordinatorCon.Close()
		oldCoordinatorCon.Close()

		requirex.Closed(t, done.Closed())
	})

	synctestx.Run(t, "release grant", func(t *testing.T) {
		coordinatorCon := newFakeCon[model.ConsumerMessage]()
		defer coordinatorCon.Close()

		done := iox.NewAsyncCloser()

		consumer := model.NewInstance(location.NewInstance(location.New("centralus", "pod1")), "endpoint")
		w, _ := model.NewWorkPool(clock.New(), consumer, service1, []model.QualifiedDomainName{domain1},
			func(ctx context.Context, self location.Instance, handler grpcx.Handler[model.ConsumerMessage, model.ConsumerMessage]) error {
				return coordinatorCon.connect(ctx, handler)
			},
			func(ctx context.Context, id model.GrantID, shard model.Shard, ownership model.Ownership) {
				ownership.RequestRevoke() // request revoke grant
				select {
				case <-done.Closed():
					return
				}
			},
		)
		defer w.Drain(10 * time.Second)

		<-coordinatorCon.Connected.Closed()

		requirex.Element(t, coordinatorCon.In)                             // register
		coordinatorCon.Out <- model.NewExtend(time.Now().Add(time.Minute)) // initial extend

		shard := model.Shard{Domain: domain1, Type: model.Unit}
		grant := model.NewGrant("grant1", shard, model.ActiveGrantState, time.Now().Add(time.Minute), time.Now())
		coordinatorCon.Out <- model.NewAssign(grant)

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
	<-f.Closed()
	return nil
}
