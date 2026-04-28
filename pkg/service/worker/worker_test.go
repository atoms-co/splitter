package worker_test

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.atoms.co/splitter/lib/service/location"
	"go.atoms.co/splitter/lib/service/session"
	"go.atoms.co/lib/testing/assertx"
	"go.atoms.co/lib/chanx"
	"go.atoms.co/lib/net/grpcx"
	"go.atoms.co/iox"
	"go.atoms.co/splitter/pkg/core"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pkg/service/coordinator"
	"go.atoms.co/splitter/pkg/service/leader"
	"go.atoms.co/splitter/pkg/service/worker"
	splitterprivatepb "go.atoms.co/splitter/pb/private"
)

var (
	service1 = model.QualifiedServiceName{Tenant: "tenant1", Service: "service1"}
	service2 = model.QualifiedServiceName{Tenant: "tenant1", Service: "service2"}
)

func TestWorker(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := t.Context()

		leaderCon := newFakeCon[leader.Message]()
		defer leaderCon.Close()

		coordinators := make(chan *fakeCoordinator)
		var cleanUpCoords []*fakeCoordinator
		defer func() {
			for _, c := range cleanUpCoords {
				c.Close()
			}
		}()

		joinFn := func(ctx context.Context, self location.Instance, handler grpcx.Handler[leader.Message, leader.Message]) error {
			return leaderCon.connect(ctx, handler)
		}
		coordFactory := func(ctx context.Context, service model.QualifiedServiceName, state core.State, updates <-chan core.Update) coordinator.Coordinator {
			c := newFakeCoordinator(service, updates)
			cleanUpCoords = append(cleanUpCoords, c)
			coordinators <- c
			return c
		}
		w, _ := worker.New(location.New("centralus", "pod1"), "endpoint", joinFn, coordFactory)
		defer func() {
			w.Drain(time.Second)
			<-w.Closed()
		}()

		<-leaderCon.Connected.Closed()

		// (1) Worker sends REGISTER
		{
			msg := assertx.Element(t, leaderCon.In)

			// << register
			wMsg, ok := msg.WorkerMessage()
			assert.True(t, ok)

			register, ok := wMsg.Register()
			assert.True(t, ok)
			assert.Len(t, register.Active(), 0)

			// >> state (omitted) and lease

			leaderCon.Out <- leader.NewLeaseUpdate(time.Now().Add(time.Minute))
		}

		// (2) Grant/Revoke
		{
			// << grant from leader --> coordinator creation
			grant := core.NewGrant("grant1", service1, time.Now().Add(time.Minute), time.Now())
			leaderCon.Out <- leader.NewAssign(grant, core.State{})
			assertx.Element(t, coordinators)

			// << grant2 from leader --> coordinator creation

			grant2 := core.NewGrant("grant2", service2, time.Now().Add(time.Minute), time.Now())
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
			service, _ := model.NewService(service2, time.Now())
			upd := core.NewServiceUpdate(model.NewServiceInfo(service, 2, time.Now()))
			leaderCon.Out <- leader.NewUpdate(grant2, upd)
			assertx.Element(t, c.updates)
		}

		// (3) Worker disconnect and reconnect to Leader
		{
			// Shut down leader connection to force reconnect
			oldLeaderCon := leaderCon
			leaderCon = newFakeCon[leader.Message]()
			oldLeaderCon.Close()

			time.Sleep(2 * time.Second) // Random backoff

			<-leaderCon.Connected.Closed()

			msg := assertx.Element(t, leaderCon.In)
			wMsg, ok := msg.WorkerMessage()
			assert.True(t, ok)
			register, ok := wMsg.Register()
			assert.True(t, ok)
			assert.Len(t, register.Active(), 1)
		}

		// (4) Consumer connects
		{
			consumer := model.NewInstance(location.NewInstance(location.New("centralus", "node")), "endpoint")
			in := chanx.NewFixed(model.NewRegister(consumer, service2, nil, nil))

			_, err := w.Connect(ctx, session.NewID(), location.NewInstance(location.New("centralus", "splitter1")), in)
			require.NoError(t, err)

			in2 := chanx.NewFixed(model.NewRegister(consumer, service1, nil, nil))
			_, err = w.Connect(ctx, session.NewID(), location.NewInstance(location.New("centralus", "splitter1")), in2)
			assert.Error(t, err)
		}
	})

}

type fakeCoordinator struct {
	iox.AsyncCloser
	t           *testing.T
	service     model.QualifiedServiceName
	updates     <-chan core.Update
	initialized iox.RAsyncCloser
}

func (f *fakeCoordinator) Self() location.Instance {
	return location.NewInstance(location.New("centralus", "pod1"))
}

func (f *fakeCoordinator) Initialized() iox.RAsyncCloser {
	return f.initialized
}

func (f *fakeCoordinator) Handle(ctx context.Context, request coordinator.HandleRequest) (*splitterprivatepb.CoordinatorHandleResponse, error) {
	return nil, nil
}

func newFakeCoordinator(service model.QualifiedServiceName, updates <-chan core.Update) *fakeCoordinator {
	i := iox.NewAsyncCloser()
	i.Close()
	return &fakeCoordinator{
		AsyncCloser: iox.NewAsyncCloser(),
		initialized: i,
		service:     service,
		updates:     updates,
	}
}

func (f *fakeCoordinator) Connect(ctx context.Context, sid session.ID, consumer location.Instance, in <-chan model.ConsumerMessage) (<-chan model.ConsumerMessage, error) {
	return nil, nil
}

func (f *fakeCoordinator) Drain(timeout time.Duration) {
	f.Close()
}
func (f *fakeCoordinator) Observe(ctx context.Context, sid session.ID, observer location.Instance, in <-chan core.ObserverClientMessage) (<-chan core.ObserverServerMessage, error) {
	return nil, nil
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
	case <-ctx.Done():
	}
	return nil
}
