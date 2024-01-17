package leader_test

import (
	"context"
	"atoms.co/lib-go/pkg/clock"
	"go.atoms.co/splitter/lib/service/location"
	"go.atoms.co/splitter/lib/service/session"
	"go.atoms.co/lib/testing/assertx"
	"go.atoms.co/lib/testing/mockclock"
	"go.atoms.co/splitter/pkg/core"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pkg/service/leader"
	"go.atoms.co/splitter/pkg/storage"
	"go.atoms.co/splitter/pkg/storage/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

const (
	tenant1  model.TenantName  = "tenant1"
	service1 model.ServiceName = "service1"
	domain1  model.DomainName  = "domain1"
)

var (
	s1 = model.QualifiedServiceName{Tenant: tenant1, Service: service1}
)

func TestLeader_SingleConsumer(t *testing.T) {
	ctx := context.Background()
	cl := mockclock.NewUnsynchronized()
	loc := location.New("centralus", "splitter-0")

	s, err := model.NewService(s1, cl.Now())
	require.NoError(t, err)
	db := setup(t, ctx, cl, s)

	l := leader.New(ctx, cl, loc, db, leader.WithFastActivation())
	<-l.Initialized().Closed()

	w := model.NewInstance(location.NewInstance(location.New("centralus", "pod1")), "endpoint")

	in := make(chan leader.Message, 1)
	in <- leader.NewRegister(w)

	out, err := l.Join(ctx, session.NewID(), in)
	require.NoError(t, err, "worker failed to join leader")

	assign := readFn(t, out, isAssign)
	assert.Equal(t, s.Name(), assign.Grant().Service())

	l.Close()
	assertx.Closed(t, out)
}

func TestLeader_SingleConsumerReattach(t *testing.T) {
	ctx := context.Background()
	cl := mockclock.NewUnsynchronized()
	loc := location.New("centralus", "splitter-0")

	s, err := model.NewService(s1, cl.Now())
	require.NoError(t, err)
	db := setup(t, ctx, cl, s)

	l := leader.New(ctx, cl, loc, db /* no need for fast activation as regrant can occur before activation */)
	<-l.Initialized().Closed()

	w := model.NewInstance(location.NewInstance(location.New("centralus", "pod1")), "endpoint")

	in := make(chan leader.Message, 1)
	in <- leader.NewRegister(w, core.NewGrant("foo", s.Name(), cl.Now().Add(20*time.Second), cl.Now()))

	out, err := l.Join(ctx, session.NewID(), in)
	require.NoError(t, err, "worker failed to join leader")

	assign := readFn(t, out, isAssign)
	assert.Equal(t, s1, assign.Grant().Service())

	l.Close()
	assertx.Closed(t, out)
}

func setup(t *testing.T, ctx context.Context, cl clock.Clock, services ...model.Service) storage.Storage {
	db := memory.New()

	for _, service := range services {
		tenant, err := model.NewTenant(service.Name().Tenant, cl.Now())
		require.NoError(t, err)

		// db updates
		err = db.Update(ctx, core.NewTenantUpdate(model.NewTenantInfo(tenant, 1, cl.Now())))
		require.NoError(t, err)
		serviceInfo := model.NewServiceInfo(service, 1, cl.Now())
		err = db.Update(ctx, core.NewServiceUpdate(serviceInfo))
	}
	return db
}

func isAssign(msg leader.Message) (leader.AssignMessage, bool) {
	if msg.IsWorkerMessage() {
		w, _ := msg.WorkerMessage()
		return w.Assign()
	}
	return leader.AssignMessage{}, false
}
func readFn[T any](t *testing.T, in <-chan leader.Message, fn func(message leader.Message) (T, bool)) T {
	t.Helper()
	for {
		select {
		case msg := <-in:
			if transform, ok := fn(msg); ok {
				return transform
			}
			continue
		case <-time.After(1 * time.Second):
			var transform T
			t.Fatal("no message read")
			return transform
		}
	}
}
