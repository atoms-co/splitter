package coordinator_test

import (
	"context"
	"atoms.co/lib-go/pkg/clock"
	"go.atoms.co/splitter/lib/service/location"
	"go.atoms.co/splitter/lib/service/session"
	"go.atoms.co/lib/testing/assertx"
	"go.atoms.co/lib/testing/mockclock"
	"go.atoms.co/splitter/pkg/core"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pkg/service/coordinator"
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
	serviceName = model.QualifiedServiceName{Tenant: tenant1, Service: service1}
	domainName  = model.QualifiedDomainName{Service: serviceName, Domain: domain1}
)

func TestCoordinator_SingleConsumer(t *testing.T) {
	ctx := context.Background()
	cl := mockclock.NewUnsynchronized()

	domain, err := model.NewDomain(domainName, model.Unit, cl.Now())
	require.NoError(t, err)

	coord := setup(ctx, cl, t, []model.Domain{domain})

	w := model.NewInstance(location.NewInstance(location.New("centralus", "pod1")), "endpoint")
	in := make(chan model.ConsumerMessage, 1)
	in <- model.NewRegister(w, serviceName, nil, nil)

	out, err := coord.Connect(ctx, session.NewID(), in)
	require.NoError(t, err, "consumer failed to join leader")

	assign := readFn(t, out, isAssign)
	assert.Len(t, assign.Grants(), 1)

	in <- model.NewDeregister()

	revoke := readFn(t, out, isRevoke)
	assert.Len(t, revoke.Grants(), 1)
	assert.Equal(t, revoke.Grants()[0].State(), model.RevokedGrantState)

	coord.Close()
	assertx.Closed(t, out)
}

func TestCoordinator_TwoConsumers(t *testing.T) {
	ctx := context.Background()
	cl := mockclock.NewUnsynchronized()

	domain, err := model.NewDomain(domainName, model.Regional, cl.Now(), model.WithDomainConfig(
		model.NewDomainConfig(
			model.WithDomainShardingPolicy(
				model.NewShardingPolicy(4)),
			model.WithDomainRegions("centralus"))),
	)
	require.NoError(t, err)

	coord := setup(ctx, cl, t, []model.Domain{domain})

	w := model.NewInstance(location.NewInstance(location.New("centralus", "pod1")), "endpoint")
	in := make(chan model.ConsumerMessage, 1)
	in <- model.NewRegister(w, serviceName, nil, nil)

	w2 := model.NewInstance(location.NewInstance(location.New("centralus", "pod2")), "endpoint")
	in2 := make(chan model.ConsumerMessage, 1)
	in2 <- model.NewRegister(w2, serviceName, nil, nil)

	out, err := coord.Connect(ctx, session.NewID(), in)
	require.NoError(t, err, "consumer1 failed to join leader")

	for i := 0; i < 4; i++ {
		assign := readFn(t, out, isAssign)
		assert.Len(t, assign.Grants(), 1)
	}

	out2, err := coord.Connect(ctx, session.NewID(), in2)
	require.NoError(t, err, "consumer2 failed to join leader")

	cl.Add(15 * time.Second) // Loadbalancing interval
	time.Sleep(50 * time.Millisecond)

	revoke := readFn(t, out, isRevoke)
	assert.Len(t, revoke.Grants(), 1)

	allocate := readFn(t, out2, isAssign) // grant allocated to consumer2
	assert.Len(t, allocate.Grants(), 1)
	assert.Equal(t, model.AllocatedGrantState, allocate.Grants()[0].State())

	in <- model.NewReleased(revoke.Grants()[0]) // consumer1 releases grant

	promote := readFn(t, out2, isPromote) // grant activated for consumer2
	assert.Len(t, promote.Grants(), 1)
	assert.Equal(t, model.ActiveGrantState, promote.Grants()[0].State())

	in <- model.NewDeregister()

	revoke1 := readFn(t, out, isRevoke)
	assert.Len(t, revoke1.Grants(), 3)

	in2 <- model.NewDeregister()

	revoke2 := readFn(t, out2, isRevoke)
	assert.Len(t, revoke2.Grants(), 1)

	coord.Close()
	assertx.Closed(t, out)
}

func setup(ctx context.Context, cl clock.Clock, t *testing.T, domains []model.Domain) coordinator.Coordinator {
	t.Helper()

	loc := location.New("centralus", "splitter-0")

	tenant, err := model.NewTenant(tenant1, cl.Now())
	require.NoError(t, err)

	service, err := model.NewService(serviceName, cl.Now())
	require.NoError(t, err)

	state := core.NewState(
		model.NewTenantInfo(tenant, 1, cl.Now()),
		[]model.ServiceInfoEx{model.NewServiceInfoEx(model.NewServiceInfo(service, 1, cl.Now()), domains)},
		nil, // TODO(jhhurwitz): 12/13/23 Test placements when implemented
	)

	updates := make(chan core.Update)

	c := coordinator.New(ctx, cl, loc, serviceName, state, updates, coordinator.WithFastActivation())
	<-c.Initialized().Closed()

	return c
}

func isAssign(msg model.ConsumerMessage) (model.AssignMessage, bool) {
	if msg.IsClientMessage() {
		c, _ := msg.ClientMessage()
		return c.Assign()
	}
	return model.AssignMessage{}, false
}

func isPromote(msg model.ConsumerMessage) (model.PromoteMessage, bool) {
	if msg.IsClientMessage() {
		c, _ := msg.ClientMessage()
		return c.Promote()
	}
	return model.PromoteMessage{}, false
}

func isRevoke(msg model.ConsumerMessage) (model.RevokeMessage, bool) {
	if msg.IsClientMessage() {
		c, _ := msg.ClientMessage()
		return c.Revoke()
	}
	return model.RevokeMessage{}, false
}

func readFn[T any](t *testing.T, in <-chan model.ConsumerMessage, fn func(message model.ConsumerMessage) (T, bool)) T {
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
