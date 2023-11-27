package worker_test

import (
	"context"
	"go.atoms.co/splitter/lib/service/location"
	"go.atoms.co/splitter/lib/service/session"
	"go.atoms.co/lib/testing/assertx"
	"go.atoms.co/lib/testing/mockclock"
	"go.atoms.co/lib/chanx"
	"go.atoms.co/lib/net/grpcx"
	"go.atoms.co/lib/iox"
	"go.atoms.co/splitter/pkg/core"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pkg/service/coordinator"
	"go.atoms.co/splitter/pkg/service/leader"
	"go.atoms.co/splitter/pkg/service/worker"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

var (
	service1 = model.QualifiedServiceName{Tenant: "tenant1", Service: "service1"}
	service2 = model.QualifiedServiceName{Tenant: "tenant1", Service: "service2"}
)

func TestWorker(t *testing.T) {
	ctx := context.Background()
	cl := mockclock.NewUnsynchronized()
	cl.Set(time.Now())

	leaderCon := newFakeCon[leader.Message]()
	defer leaderCon.Close()

	coordinators := make(chan *fakeCoordinator)

	loc := location.NewInstance(location.New("centralus", "pod1"))
	w, _ := worker.New(cl, model.NewInstance(loc, "endpoint"),
		func(ctx context.Context, handler grpcx.Handler[leader.Message, leader.Message]) error {
			return leaderCon.connect(ctx, handler)
		},
		func(ctx context.Context, service model.QualifiedServiceName, state core.State, updates <-chan core.Update) coordinator.Coordinator {
			c := newFakeCoordinator(service, updates)
			coordinators <- c
			return c
		},
	)
	defer w.Drain(time.Second)

	<-leaderCon.Connected.Closed()

	// (1) Worker sends REGISTER

	t.Run("register", func(t *testing.T) {
		msg := assertx.Element(t, leaderCon.In)

		// << register
		wMsg, ok := msg.WorkerMessage()
		assert.True(t, ok)

		register, ok := wMsg.Register()
		assert.True(t, ok)
		assert.Len(t, register.Active(), 0)

		// >> state (omitted) and lease

		leaderCon.Out <- leader.NewLeaseUpdate(cl.Now().Add(time.Minute))
	})

	// (2) Grant/Revoke

	t.Run("grant", func(t *testing.T) {
		// << grant from leader --> coordinator creation
		grant := core.NewGrant("grant1", service1, cl.Now().Add(time.Minute), cl.Now())
		leaderCon.Out <- leader.NewAssign(grant, core.State{})
		assertx.Element(t, coordinators)

		// << grant2 from leader --> coordinator creation

		grant2 := core.NewGrant("grant2", service2, cl.Now().Add(time.Minute), cl.Now())
		leaderCon.Out <- leader.NewAssign(grant2, core.State{})
		c := assertx.Element(t, coordinators)

		// << revoke grant1 from leader --> relinquish

		leaderCon.Out <- leader.NewRevoke(grant)

		// >> relinquished

		msg := assertx.Element(t, leaderCon.In)
		wMsg, ok := msg.WorkerMessage()
		assert.True(t, ok)
		assert.True(t, wMsg.IsRelinquished())

		// update
		service, _ := model.NewService(service2, cl.Now())
		upd := core.NewServiceUpdate(model.NewServiceInfo(service, 2, cl.Now()))
		leaderCon.Out <- leader.NewUpdate(grant2, upd)
		assertx.Element(t, c.updates)
	})

	// (2) Grant/Revoke

	t.Run("update", func(t *testing.T) {

	})

	// (3) Worker disconnect and reconnect to Leader

	t.Run("worker/disconnect", func(t *testing.T) {
		leaderCon.Out <- leader.NewDisconnect()

		// Shut down leader connection to force reconnect
		oldLeaderCon := leaderCon
		leaderCon = newFakeCon[leader.Message]()
		oldLeaderCon.Close()

		time.Sleep(50 * time.Millisecond)
		cl.Add(2 * time.Second) // Random backoff

		<-leaderCon.Connected.Closed()

		msg := assertx.Element(t, leaderCon.In)
		wMsg, ok := msg.WorkerMessage()
		assert.True(t, ok)
		register, ok := wMsg.Register()
		assert.True(t, ok)
		assert.Len(t, register.Active(), 1)
	})

	// (4) Consumer connects

	t.Run("consumer/connect", func(t *testing.T) {
		consumer := model.NewInstance(location.NewInstance(location.New("centralus", "node")), "endpoint")
		in := chanx.NewFixed(model.NewConsumerRegisterMessage(model.NewRegisterMessage(consumer, service2, nil, nil)))

		_, err := w.Connect(ctx, session.NewID(), in)
		require.NoError(t, err)

		in2 := chanx.NewFixed(model.NewConsumerRegisterMessage(model.NewRegisterMessage(consumer, service1, nil, nil)))
		_, err = w.Connect(ctx, session.NewID(), in2)
		assert.Error(t, err)
	})

}

type fakeCoordinator struct {
	iox.AsyncCloser
	t       *testing.T
	service model.QualifiedServiceName
	updates <-chan core.Update
}

func newFakeCoordinator(service model.QualifiedServiceName, updates <-chan core.Update) *fakeCoordinator {
	return &fakeCoordinator{
		AsyncCloser: iox.NewAsyncCloser(),
		service:     service,
		updates:     updates,
	}
}

func (f *fakeCoordinator) Connect(ctx context.Context, sid session.ID, register model.RegisterMessage, in <-chan model.ConsumerMessage) (<-chan model.ConsumerMessage, error) {
	if register.Service() != f.service {
		return nil, fmt.Errorf("service doesnt match: expected %v, received %v", f.service, register.Service())
	}
	return nil, nil
}

func (f *fakeCoordinator) Drain(timeout time.Duration) {
	f.Close()
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
