package leader_test

import (
	"context"
	"go.atoms.co/splitter/lib/service/location"
	"go.atoms.co/splitter/lib/service/session"
	"go.atoms.co/lib/testing/assertx"
	"go.atoms.co/lib/testing/mockclock"
	"go.atoms.co/splitter/pkg/core"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pkg/service/leader"
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

func TestLeader(t *testing.T) {
	ctx := context.Background()
	cl := mockclock.NewUnsynchronized()
	loc := location.New("centralus", "splitter-0")
	db := memory.New()

	tenant, err := model.NewTenant(tenant1, cl.Now())
	require.NoError(t, err)

	serviceName := model.QualifiedServiceName{Tenant: tenant1, Service: service1}
	service, err := model.NewService(serviceName, cl.Now())
	require.NoError(t, err)

	domainName := model.QualifiedDomainName{Service: serviceName, Domain: domain1}
	domain, err := model.NewDomain(domainName, model.Unit, cl.Now())
	require.NoError(t, err)

	err = db.Update(ctx, core.NewTenantUpdate(model.NewTenantInfo(tenant, 1, cl.Now())))
	require.NoError(t, err)

	serviceInfo := model.NewServiceInfo(service, 1, cl.Now())
	err = db.Update(ctx, core.NewServiceUpdate(serviceInfo))
	require.NoError(t, err)

	err = db.Update(ctx, core.NewDomainUpdate(model.NewServiceInfo(service, 2, cl.Now()), domain))
	require.NoError(t, err)

	l := leader.New(ctx, cl, loc, db, leader.WithFastActivation())
	<-l.Initialized().Closed()

	w := model.NewInstance(location.NewInstance(location.New("centralus", "pod1")), "endpoint")

	in := make(chan leader.Message, 1)
	in <- leader.NewRegister(w)

	out, err := l.Join(ctx, session.NewID(), in)
	require.NoError(t, err, "worker failed to join leader")

	assign := readFn(t, out, isAssign)
	assert.Equal(t, serviceName, assign.Grant().Service())

	l.Close()
	assertx.Closed(t, out)
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
