package model_test

import (
	"context"
	"go.atoms.co/splitter/lib/service/location"
	"go.atoms.co/lib/testing/assertx"
	"go.atoms.co/lib/testing/mockclock"
	"go.atoms.co/lib/net/grpcx"
	"go.atoms.co/lib/iox"
	"go.atoms.co/splitter/pkg/model"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

var (
	service1 = model.QualifiedServiceName{Tenant: "tenant1", Service: "service1"}
	domain1  = model.QualifiedDomainName{Service: service1, Domain: "domain1"}
)

func TestWorkpool(t *testing.T) {
	cl := mockclock.NewUnsynchronized()
	cl.Set(time.Now())

	coordinatorCon := newFakeCon[model.ConsumerMessage]()
	defer coordinatorCon.Close()

	shards := make(chan model.Shard)

	consumer := model.NewInstance(location.NewInstance(location.New("centralus", "pod1")), "endpoint")
	w, _ := model.NewWorkPool(cl, consumer, service1, []model.QualifiedDomainName{domain1},
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

	// (1) Workpool sends REGISTER

	t.Run("register", func(t *testing.T) {
		msg := assertx.Element(t, coordinatorCon.In)

		// << register
		cMsg, ok := msg.ClientMessage()
		assert.True(t, ok)

		register, ok := cMsg.Register()
		assert.True(t, ok)
		assert.Len(t, register.Active(), 0)

		// >> state (omitted) and lease

		coordinatorCon.Out <- model.NewExtend(cl.Now().Add(time.Minute))
	})

	// (2) Grant/Revoke

	t.Run("grant", func(t *testing.T) {
		// << grant from coordinator --> coordinator creation

		shard := model.Shard{Domain: domain1, Type: model.Unit}
		grant := model.NewGrant("grant1", shard, model.ActiveGrantState, cl.Now().Add(time.Minute), cl.Now())
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

	t.Run("workpool/disconnect", func(t *testing.T) {
		// grant
		shard := model.Shard{Domain: domain1, Type: model.Unit}
		grant := model.NewGrant("grant1", shard, model.ActiveGrantState, cl.Now().Add(time.Minute), cl.Now())
		coordinatorCon.Out <- model.NewAssign(grant)
		assertx.Element(t, shards)

		// Shut down coordinator connection to force reconnect
		oldCoordinatorCon := coordinatorCon
		coordinatorCon = newFakeCon[model.ConsumerMessage]()
		oldCoordinatorCon.Close()

		time.Sleep(50 * time.Millisecond)
		cl.Add(2 * time.Second) // Random backoff

		<-coordinatorCon.Connected.Closed()

		msg := assertx.Element(t, coordinatorCon.In)
		cMsg, ok := msg.ClientMessage()
		assert.True(t, ok)
		register, ok := cMsg.Register()
		assert.True(t, ok)
		assert.Len(t, register.Active(), 1)

		coordinatorCon.Out <- model.NewRevoke(grant)
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
