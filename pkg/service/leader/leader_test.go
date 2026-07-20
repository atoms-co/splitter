package leader_test

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
	"go.atoms.co/splitter/pkg/core"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pkg/service/leader"
	"go.atoms.co/splitter/pkg/storage"
	"go.atoms.co/splitter/pkg/storage/memory"
	splitterprivatepb "go.atoms.co/splitter/pb/private"
	splitterpb "go.atoms.co/splitter/pb"
	"go.atoms.co/lib/chanx"
)

const (
	tenant1  model.TenantName  = "tenant1"
	tenant2  model.TenantName  = "tenant2"
	service1 model.ServiceName = "service1"
	service2 model.ServiceName = "service2"
)

var (
	s1 = model.QualifiedServiceName{Tenant: tenant1, Service: service1}
	s2 = model.QualifiedServiceName{Tenant: tenant2, Service: service2}
)

func TestLeader_SingleWorker(t *testing.T) {
	ctx := context.Background()
	loc := location.New("centralus", "splitter-0")

	s, err := model.NewService(s1, time.Now())
	require.NoError(t, err)

	db := setup(t, ctx, s)

	l := leader.New(ctx, loc, db, leader.WithFastActivation())
	<-l.Initialized().Closed()

	w := model.NewInstance(location.NewInstance(location.New("centralus", "pod1")), "endpoint")

	in := make(chan leader.Message, 1)
	in <- leader.NewRegister(w)

	out, err := l.Join(ctx, session.NewID(), in)
	require.NoError(t, err, "worker failed to join leader")

	assign := readFn(t, out, isAssign)
	assert.Equal(t, s.Name(), assign.Grant().Service())

	in <- leader.NewDeregister()

	revoke := readFn(t, out, isRevoke)
	assert.Len(t, revoke.Grants(), 1)

	l.Close()
	assertx.Closed(t, out)
}

func TestLeader_SingleWorkerReattach(t *testing.T) {
	ctx := context.Background()
	loc := location.New("centralus", "splitter-0")

	s, err := model.NewService(s1, time.Now())
	require.NoError(t, err)

	db := setup(t, ctx, s)

	l := leader.New(ctx, loc, db /* no need for fast activation as regrant can occur before activation */)
	<-l.Initialized().Closed()

	w := model.NewInstance(location.NewInstance(location.New("centralus", "pod1")), "endpoint")

	in := make(chan leader.Message, 1)
	in <- leader.NewRegister(w, core.NewGrant("foo", s.Name(), time.Now().Add(20*time.Second), time.Now()))

	out, err := l.Join(ctx, session.NewID(), in)
	require.NoError(t, err, "worker failed to join leader")

	assign := readFn(t, out, isAssign)
	assert.Equal(t, s1, assign.Grant().Service())

	in <- leader.NewDeregister()

	revoke := readFn(t, out, isRevoke)
	assert.Len(t, revoke.Grants(), 1)

	l.Close()
	assertx.Closed(t, out)
}

func TestLeader_MultipleWorker(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		loc := location.New("centralus", "splitter-0")

		s1, err := model.NewService(s1, time.Now(), model.WithServiceConfig(model.NewServiceConfig(model.WithServiceRegion("centralus"))))
		require.NoError(t, err)
		s2, err := model.NewService(s2, time.Now(), model.WithServiceConfig(model.NewServiceConfig(model.WithServiceRegion("northcentralus"))))
		require.NoError(t, err)

		db := setup(t, ctx, s1, s2)

		l := leader.New(ctx, loc, db, leader.WithFastActivation())
		<-l.Initialized().Closed()

		w1 := model.NewInstance(location.NewInstance(location.New("centralus", "pod1")), "endpoint")

		in1 := make(chan leader.Message, 1)
		in1 <- leader.NewRegister(w1)

		// Worker 1 joins and receives both assignment

		out1, err := l.Join(ctx, session.NewID(), in1)
		require.NoError(t, err, "worker 1 failed to join leader")

		readFn(t, out1, isAssign)
		readFn(t, out1, isAssign)

		w2 := model.NewInstance(location.NewInstance(location.New("northcentralus", "pod1")), "endpoint")

		in2 := make(chan leader.Message, 1)
		in2 <- leader.NewRegister(w2)

		// Worker 2 joins and receives an assignment after rebalance

		out2, err := l.Join(ctx, session.NewID(), in2)
		require.NoError(t, err, "worker 2 failed to join leader")

		time.Sleep(10 * time.Second) // advance to rebalance

		revoke := readFn(t, out1, isRevoke)
		assert.Len(t, revoke.Grants(), 1)
		assert.Equal(t, s2.Name(), revoke.Grants()[0].Service()) // Should revoke service with regional preference for w2

		in1 <- leader.NewRelinquished(revoke.Grants()[0])

		assign := readFn(t, out2, isAssign)
		assert.Equal(t, s2.Name(), assign.Grant().Service())

		// Both Worker 1 and Worker 2 register and receive revokes

		in1 <- leader.NewDeregister()

		revoke = readFn(t, out1, isRevoke)
		assert.Len(t, revoke.Grants(), 1)

		in2 <- leader.NewDeregister()

		revoke = readFn(t, out2, isRevoke)
		assert.Len(t, revoke.Grants(), 1)

		l.Close()
		assertx.Closed(t, out1)
	})
}

func TestLeader_Operations(t *testing.T) {
	ctx := context.Background()
	loc := location.New("centralus", "splitter-0")

	s1, err := model.NewService(s1, time.Now(), model.WithServiceConfig(model.NewServiceConfig(model.WithServiceRegion("centralus"))))
	require.NoError(t, err)
	s2, err := model.NewService(s2, time.Now(), model.WithServiceConfig(model.NewServiceConfig(model.WithServiceRegion("northcentralus"))))
	require.NoError(t, err)

	db := setup(t, ctx, s1, s2)

	l := leader.New(ctx, loc, db, leader.WithFastActivation())
	<-l.Initialized().Closed()

	resp, err := l.Handle(ctx, leader.NewHandleOperationRequest(&splitterprivatepb.OperationRequest{
		Req: &splitterprivatepb.OperationRequest_Snapshot{Snapshot: &splitterprivatepb.SnapshotRequest{}},
	}))
	require.NoError(t, err)
	require.NotNil(t, resp.GetOperation())

	snap := resp.GetOperation().GetSnapshot()
	require.NotNil(t, snap)

	assert.Len(t, snap.GetSnapshot().GetTenants(), 2)
}

func TestLeader_HandleUpdate(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		loc := location.New("centralus", "splitter-0")

		cfg := model.NewServiceConfig(model.WithTrackLoad(true))
		service, err := model.NewService(s1, time.Now(), model.WithServiceConfig(cfg))
		require.NoError(t, err)

		qdn := model.QualifiedDomainName{Service: service.Name(), Domain: model.DomainName("domain")}
		domain, err := model.NewDomain(qdn, model.Unit, time.Now())
		require.NoError(t, err)
		db := setupWithDomains(t, ctx, service, domain)

		l := leader.New(ctx, loc, db, leader.WithFastActivation())
		defer l.Close()
		<-l.Initialized().Closed()

		worker := model.NewInstance(location.NewInstance(location.New("centralus", "pod1")), "endpoint")
		in := make(chan leader.Message, 2)
		in <- leader.NewRegister(worker)

		out, err := l.Join(ctx, session.NewID(), in)
		require.NoError(t, err, "worker failed to join leader")
		readFn(t, out, isAssign)
		synctest.Wait()
		chanx.Clear(out)

		// (1) Test Status update
		snap := core.NewP2QuantileSnapshot(
			0.5,
			[]float64{10, 10, 10, 10, 10},
			[]uint64{1, 2, 3, 4, 10},
			[]float64{1, 2, 3, 4, 10},
		)
		trackers := core.NewDomainTrackerSnapshot(time.Now(), snap, nil)
		status := core.NewServiceStatusMessage(core.NewServiceLoadInfo(service.Name(), []core.DomainLoadInfo{
			core.NewDomainLoadInfo(domain.Name().Domain, trackers, nil),
		}))
		in <- leader.NewServiceStatus(status)
		synctest.Wait()

		resp, err := l.Handle(ctx, leader.NewHandleOperationRequest(&splitterprivatepb.OperationRequest{
			Req: &splitterprivatepb.OperationRequest_Snapshot{Snapshot: &splitterprivatepb.SnapshotRequest{}},
		}))
		require.NoError(t, err)
		require.NotNil(t, resp.GetOperation())
		synctest.Wait()

		// snapshot contains ServiceStatus
		snapshot := core.WrapSnapshot(resp.GetOperation().GetSnapshot().GetSnapshot())
		require.True(t, snapshotHasServiceStatus(snapshot, service.Name(), domain.Name()))

		// no message sent to worker
		_, ok := chanx.TryRead(out, time.Millisecond)
		require.False(t, ok)

		// (2) Test State update
		resp, err = l.Handle(ctx, leader.NewHandleServiceRequest(&splitterprivatepb.ServiceRequest{
			Req: &splitterprivatepb.ServiceRequest_Update{
				Update: &splitterpb.UpdateServiceRequest{
					Name:    service.Name().ToProto(),
					Version: int64(snapshot.Tenants()[0].Services()[0].Info().Version()),
					Config:  model.UnwrapServiceConfig(model.NewServiceConfig(model.WithTrackLoad(false))),
				},
			},
		}))
		require.NoError(t, err)
		require.NotNil(t, resp.GetService().GetUpdate())

		// Update message send to worker
		update := readFn(t, out, isUpdate)
		assert.Equal(t, service.Name(), update.Grant().Service())
		assert.True(t, update.State().IsStateUpdated())
	})
}

func setup(t *testing.T, ctx context.Context, services ...model.Service) storage.Storage {
	db := memory.New()

	for _, service := range services {
		tenant, err := model.NewTenant(service.Name().Tenant, time.Now())
		require.NoError(t, err)

		// db updates
		err = db.Update(ctx, core.NewTenantUpdate(model.NewTenantInfo(tenant, 1, time.Now())))
		require.NoError(t, err)
		serviceInfo := model.NewServiceInfo(service, 1, time.Now())
		err = db.Update(ctx, core.NewServiceUpdate(serviceInfo))
	}
	return db
}

func setupWithDomains(t *testing.T, ctx context.Context, service model.Service, domains ...model.Domain) storage.Storage {
	db := memory.New()

	tenant, err := model.NewTenant(service.Name().Tenant, time.Now())
	require.NoError(t, err)

	err = db.Update(ctx, core.NewTenantUpdate(model.NewTenantInfo(tenant, 1, time.Now())))
	require.NoError(t, err)

	serviceInfo := model.NewServiceInfo(service, 1, time.Now())
	err = db.Update(ctx, core.NewServiceUpdate(serviceInfo))
	require.NoError(t, err)

	for _, domain := range domains {
		serviceInfo = model.NewServiceInfo(service, serviceInfo.Version()+1, time.Now())
		err = db.Update(ctx, core.NewDomainUpdate(serviceInfo, domain))
		require.NoError(t, err)
	}

	return db
}

func snapshotHasServiceStatus(snapshot core.Snapshot, service model.QualifiedServiceName, domain model.QualifiedDomainName) bool {
	for _, state := range snapshot.Tenants() {
		for _, status := range state.Statuses() {
			load := status.Load()
			if load.Service() != service {
				continue
			}

			for _, domainLoad := range load.Domains() {
				if domainLoad.DomainName() == domain.Domain {
					return true
				}
			}
		}
	}
	return false
}

func isAssign(msg leader.Message) (leader.AssignMessage, bool) {
	if msg.IsWorkerMessage() {
		w, _ := msg.WorkerMessage()
		return w.Assign()
	}
	return leader.AssignMessage{}, false
}

func isRevoke(msg leader.Message) (leader.RevokeMessage, bool) {
	if msg.IsWorkerMessage() {
		w, _ := msg.WorkerMessage()
		return w.Revoke()
	}
	return leader.RevokeMessage{}, false
}

func isUpdate(msg leader.Message) (leader.UpdateMessage, bool) {
	if msg.IsWorkerMessage() {
		w, _ := msg.WorkerMessage()
		return w.Update()
	}
	return leader.UpdateMessage{}, false
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
