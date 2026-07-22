package raft

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	hashicorpraft "github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"

	"go.atoms.co/lib/encoding/protox"
	splitterprivatepb "go.atoms.co/splitter/pb/private"
	"go.atoms.co/splitter/pkg/core"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/testing/prefab"
)

func TestStorage_UpdateServiceStatusThroughRaft(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		now := time.Now()
		db, logStore, cleanup := newRaftStorage(t)
		defer cleanup()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// setup tenant, service and domain
		serviceName := model.QualifiedServiceName{
			Tenant:  model.TenantName("tenant"),
			Service: model.ServiceName("service"),
		}
		domainName := prefab.QDN("tenant/service/domain")

		tenant, err := model.NewTenant(serviceName.Tenant, now)
		require.NoError(t, err)
		require.NoError(t, db.Update(ctx, core.NewTenantUpdate(model.NewTenantInfo(tenant, 1, now))))

		serviceCfg := model.NewServiceConfig(model.WithTrackLoad(true))
		service, err := model.NewService(serviceName, now, model.WithServiceConfig(serviceCfg))
		require.NoError(t, err)
		serviceInfo := model.NewServiceInfo(service, 1, now)
		require.NoError(t, db.Update(ctx, core.NewServiceUpdate(serviceInfo)))

		domain, err := model.NewDomain(domainName, model.Unit, now)
		require.NoError(t, err)
		serviceInfo = model.NewServiceInfo(service, 2, now)
		require.NoError(t, db.Update(ctx, core.NewDomainUpdate(serviceInfo, domain)))

		// store ServiceStatus
		snap := core.NewP2QuantileSnapshot(
			0.5,
			[]float64{10, 10, 10, 10, 10},
			[]uint64{1, 2, 3, 4, 10},
			[]float64{1, 2, 3, 4, 10},
		)
		tracker := core.NewDomainTrackerSnapshot(now, snap, nil)
		domainLoadInfo := core.NewDomainLoadInfo(domainName.Domain, tracker, nil)
		status := core.NewServiceStatus(core.NewServiceLoadInfo(serviceName, []core.DomainLoadInfo{
			domainLoadInfo,
		}))
		require.NoError(t, db.Update(ctx, core.NewServiceStatusUpdate(status)))

		// require ServiceStatus in raft log
		lastIndex, err := logStore.LastIndex()
		require.NoError(t, err)

		var entry hashicorpraft.Log
		require.NoError(t, logStore.GetLog(lastIndex, &entry))

		mutation, err := protox.Unmarshal[splitterprivatepb.Mutation](entry.Data)
		require.NoError(t, err)
		require.NotNil(t, mutation.GetUpdate())

		actual, ok := core.WrapUpdate(mutation.GetUpdate()).ServiceStatus()
		require.True(t, ok)
		require.True(t, status.Equal(actual))

		// require ServiceStatus in snapshot
		snapshot, err := db.Read(ctx)
		require.NoError(t, err)

		require.Len(t, snapshot.Tenants(), 1)
		require.Len(t, snapshot.Tenants()[0].Statuses(), 1)
		require.True(t, status.Equal(snapshot.Tenants()[0].Statuses()[0]))
	})
}

func newRaftStorage(t *testing.T) (*Storage, *hashicorpraft.InmemStore, func()) {
	t.Helper()

	const serverID = hashicorpraft.ServerID("node1")

	cfg := hashicorpraft.DefaultConfig()
	cfg.LocalID = serverID

	fsm := NewFSM()
	logStore := hashicorpraft.NewInmemStore()
	stableStore := hashicorpraft.NewInmemStore()
	snapshotStore := hashicorpraft.NewInmemSnapshotStore()
	addr, transport := hashicorpraft.NewInmemTransport(hashicorpraft.NewInmemAddr())

	r, err := hashicorpraft.NewRaft(cfg, fsm, logStore, stableStore, snapshotStore, transport)
	require.NoError(t, err)

	require.NoError(t, r.BootstrapCluster(hashicorpraft.Configuration{
		Servers: []hashicorpraft.Server{{
			ID:      serverID,
			Address: addr,
		}},
	}).Error())

	// A node has state transition Follower -> Candidate -> Leader before becoming the leader.
	// Wait leader election to finish.
	for range 50 {
		synctest.Wait()
		_, leaderID := r.LeaderWithID()
		if r.State() == hashicorpraft.Leader && leaderID == serverID {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	require.Equal(t, hashicorpraft.Leader, r.State())
	_, leaderID := r.LeaderWithID()
	require.Equal(t, serverID, leaderID)

	cleanup := func() {
		require.NoError(t, r.Shutdown().Error())
		require.NoError(t, transport.Close())
	}

	return New(serverID, r, fsm), logStore, cleanup
}
