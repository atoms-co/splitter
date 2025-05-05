package coordinator

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"atoms.co/lib-go/pkg/clock"
	"go.atoms.co/splitter/lib/service/location"
	"go.atoms.co/splitter/lib/service/session"
	"go.atoms.co/lib/testing/assertx"
	"go.atoms.co/lib/testing/mockclock"
	"go.atoms.co/lib/uuidx"
	"go.atoms.co/splitter/pkg/core"
	"go.atoms.co/splitter/pkg/model"
	splitterprivatepb "go.atoms.co/splitter/pb/private"
)

const (
	tenant1  model.TenantName  = "tenant1"
	service1 model.ServiceName = "service1"
	domain1  model.DomainName  = "domain1"
	domain2  model.DomainName  = "domain2"
	domain3  model.DomainName  = "domain3"
)

var (
	serviceName = model.QualifiedServiceName{Tenant: tenant1, Service: service1}
	domainName  = model.QualifiedDomainName{Service: serviceName, Domain: domain1}
	domainName2 = model.QualifiedDomainName{Service: serviceName, Domain: domain2}
	domainName3 = model.QualifiedDomainName{Service: serviceName, Domain: domain3}
)

func TestCoordinator_SingleConsumer(t *testing.T) {
	ctx := context.Background()
	cl := mockclock.NewUnsynchronized()

	domain, err := model.NewDomain(domainName, model.Unit, cl.Now())
	require.NoError(t, err)

	coord := setup(ctx, cl, t, []model.Domain{domain}, true)

	w := model.NewInstance(location.NewInstance(location.New("centralus", "pod1")), "endpoint")
	in := make(chan model.ConsumerMessage, 1)
	in <- model.NewRegister(w, serviceName, nil, nil)

	out, err := coord.Connect(ctx, session.NewID(), location.NewInstance(location.New("centralus", "splitter1")), in)
	require.NoError(t, err, "consumer failed to join leader")

	snapshot := readFn(t, out, isClusterSnapshot)
	assert.Len(t, snapshot.Assignments(), 0)

	assign := readFn(t, out, isAssign)
	assert.Len(t, assign.Grants(), 1)

	change := readFn(t, out, isClusterChange)
	assert.Len(t, change.Assign().Assignments(), 1)

	in <- model.NewDeregister()

	revoke := readFn(t, out, isRevoke)
	assert.Len(t, revoke.Grants(), 1)
	assert.Equal(t, revoke.Grants()[0].State(), model.RevokedGrantState)

	change = readFn(t, out, isClusterChange)
	assert.Len(t, change.Update().Grants(), 1)
	assert.Equal(t, change.Update().Grants()[0].State(), model.RevokedGrantState)

	in <- model.NewReleased(revoke.Grants()[0])

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

	coord := setup(ctx, cl, t, []model.Domain{domain}, true)

	w := model.NewInstance(location.NewInstance(location.New("centralus", "pod1")), "endpoint")
	in := make(chan model.ConsumerMessage, 1)
	in <- model.NewRegister(w, serviceName, nil, nil)

	w2 := model.NewInstance(location.NewInstance(location.New("centralus", "pod2")), "endpoint")
	in2 := make(chan model.ConsumerMessage, 1)
	in2 <- model.NewRegister(w2, serviceName, nil, nil)

	out, err := coord.Connect(ctx, session.NewID(), location.NewInstance(location.New("centralus", "splitter1")), in)
	require.NoError(t, err, "consumer1 failed to join leader")

	snapshot := readFn(t, out, isClusterSnapshot)
	assert.Len(t, snapshot.Assignments(), 0)

	for i := 0; i < 4; i++ {
		assign := readFn(t, out, isAssign)
		assert.Len(t, assign.Grants(), 1)
	}

	change := readFn(t, out, isClusterChange)
	assert.Len(t, change.Remove().Consumers(), 0)
	assert.Len(t, change.Unassign().Grants(), 0)
	assert.Len(t, change.Update().Grants(), 0)
	assert.Len(t, change.Assign().Assignments(), 1)

	out2, err := coord.Connect(ctx, session.NewID(), location.NewInstance(location.New("centralus", "splitter1")), in2)
	require.NoError(t, err, "consumer2 failed to join leader")

	snapshot = readFn(t, out2, isClusterSnapshot)
	assert.Len(t, snapshot.Assignments(), 1)

	cl.Add(15 * time.Second) // Loadbalancing interval
	time.Sleep(50 * time.Millisecond)

	revoke := readFn(t, out, isRevoke)
	assert.Len(t, revoke.Grants(), 1)

	allocate := readFn(t, out2, isAssign) // grant allocated to consumer2
	assert.Len(t, allocate.Grants(), 1)
	assert.Equal(t, model.AllocatedGrantState, allocate.Grants()[0].State())

	change = readFn(t, out, isClusterChange)
	assert.Len(t, change.Remove().Consumers(), 0)
	assert.Len(t, change.Unassign().Grants(), 0)
	assert.Len(t, change.Update().Grants(), 1)
	assert.Equal(t, change.Update().Grants()[0].State(), model.RevokedGrantState)
	assert.Len(t, change.Assign().Assignments(), 1)

	change = readFn(t, out2, isClusterChange)
	assert.Len(t, change.Remove().Consumers(), 0)
	assert.Len(t, change.Unassign().Grants(), 0)
	assert.Len(t, change.Update().Grants(), 1)
	assert.Equal(t, change.Update().Grants()[0].State(), model.RevokedGrantState)
	assert.Len(t, change.Assign().Assignments(), 1)

	in <- model.NewReleased(revoke.Grants()[0]) // consumer1 releases grant

	promote := readFn(t, out2, isPromote) // grant activated for consumer2
	assert.Len(t, promote.Grants(), 1)
	assert.Equal(t, model.ActiveGrantState, promote.Grants()[0].State())

	cl.Add(300 * time.Millisecond) // 2x Broadcast interval
	time.Sleep(50 * time.Millisecond)

	change = readFn(t, out, isClusterChange)
	assert.Len(t, change.Remove().Consumers(), 0)
	assert.Len(t, change.Unassign().Grants(), 1)
	assert.Len(t, change.Update().Grants(), 1)
	assert.Equal(t, change.Update().Grants()[0].State(), model.ActiveGrantState)
	assert.Len(t, change.Assign().Assignments(), 0)

	change = readFn(t, out2, isClusterChange)
	assert.Len(t, change.Remove().Consumers(), 0)
	assert.Len(t, change.Unassign().Grants(), 1)
	assert.Len(t, change.Update().Grants(), 1)
	assert.Equal(t, change.Update().Grants()[0].State(), model.ActiveGrantState)
	assert.Len(t, change.Assign().Assignments(), 0)

	in <- model.NewDeregister()

	revoke1 := readFn(t, out, isRevoke)
	assert.Len(t, revoke1.Grants(), 3)

	change = readFn(t, out, isClusterChange)
	assert.Len(t, change.Remove().Consumers(), 0)
	assert.Len(t, change.Unassign().Grants(), 0)
	assert.Len(t, change.Update().Grants(), 3)
	assert.Len(t, change.Assign().Assignments(), 1)

	change = readFn(t, out2, isClusterChange)
	assert.Len(t, change.Remove().Consumers(), 0)
	assert.Len(t, change.Unassign().Grants(), 0)
	assert.Len(t, change.Update().Grants(), 3)
	assert.Len(t, change.Assign().Assignments(), 1)

	in <- model.NewReleased(revoke1.Grants()...)

	cl.Add(300 * time.Millisecond) // 2x Broadcast interval
	time.Sleep(50 * time.Millisecond)

	for i := 0; i < 3; i++ {
		promote = readFn(t, out2, isPromote)
		assert.Len(t, promote.Grants(), 1)
	}

	change = readFn(t, out2, isClusterChange)
	assert.Len(t, change.Remove().Consumers(), 1)
	assert.Len(t, change.Unassign().Grants(), 0)
	assert.Len(t, change.Update().Grants(), 3)
	assert.Len(t, change.Assign().Assignments(), 0)

	in2 <- model.NewDeregister()

	revoke2 := readFn(t, out2, isRevoke)
	assert.Len(t, revoke2.Grants(), 4)

	coord.Close()
	assertx.Closed(t, out)
}

func TestCoordinator_TwoConsumers_IgnoreLoadBalanceUnitDomain2(t *testing.T) {
	ctx := context.Background()
	cl := mockclock.NewUnsynchronized()

	domain, err := model.NewDomain(domainName, model.Regional, cl.Now(), model.WithDomainConfig(
		model.NewDomainConfig(
			model.WithDomainShardingPolicy(
				model.NewShardingPolicy(1)),
			model.WithDomainRegions("centralus"))),
	)
	require.NoError(t, err)

	unit, err := model.NewDomain(domainName2, model.Unit, cl.Now())
	require.NoError(t, err)
	unit2, err := model.NewDomain(domainName3, model.Unit, cl.Now())
	require.NoError(t, err)

	// (1) Setup 1 regional domain "domain1" and 2 unit "domain1" + "domain2". 1 shard each.

	coord := setup(ctx, cl, t, []model.Domain{domain, unit, unit2}, true)

	w := model.NewInstance(location.NewInstance(location.New("centralus", "pod1")), "endpoint")
	in := make(chan model.ConsumerMessage, 1)
	in <- model.NewRegister(w, serviceName, nil, nil)

	// (2) Connect w1. It should receive all shards

	out, err := coord.Connect(ctx, session.NewID(), location.NewInstance(location.New("centralus", "splitter1")), in)
	require.NoError(t, err, "consumer1 failed to join leader")

	snapshot := readFn(t, out, isClusterSnapshot)
	assert.Len(t, snapshot.Assignments(), 0)

	for i := 0; i < 3; i++ {
		assign := readFn(t, out, isAssign)
		assert.Len(t, assign.Grants(), 1)
	}

	change := readFn(t, out, isClusterChange)
	assert.Len(t, change.Remove().Consumers(), 0)
	assert.Len(t, change.Unassign().Grants(), 0)
	assert.Len(t, change.Update().Grants(), 0)
	assert.Len(t, change.Assign().Assignments(), 1)

	// (3) Connect w2. Load-balance should move the regional domain shard. Unit should not move.

	w2 := model.NewInstance(location.NewInstance(location.New("centralus", "pod2")), "endpoint")
	in2 := make(chan model.ConsumerMessage, 1)
	in2 <- model.NewRegister(w2, serviceName, nil, nil)

	out2, err := coord.Connect(ctx, session.NewID(), location.NewInstance(location.New("centralus", "splitter1")), in2)
	require.NoError(t, err, "consumer2 failed to join leader")

	snapshot = readFn(t, out2, isClusterSnapshot)
	assert.Len(t, snapshot.Assignments(), 1)

	cl.Add(15 * time.Second) // Loadbalancing interval
	time.Sleep(50 * time.Millisecond)

	// Loadbalance generally prefers moving a heavy shard first. But it should not move Unit domains.

	revoke := readFn(t, out, isRevoke) // grant (domain1) revoked from consumer1
	assert.Len(t, revoke.Grants(), 1)
	assert.Equal(t, model.RevokedGrantState, revoke.Grants()[0].State())
	assert.Equal(t, domainName, revoke.Grants()[0].Shard().Domain)
	assert.Equal(t, model.Regional, revoke.Grants()[0].Shard().Type)

	allocate := readFn(t, out2, isAssign) // grant (domain1) allocated to consumer2
	assert.Len(t, allocate.Grants(), 1)
	assert.Equal(t, model.AllocatedGrantState, allocate.Grants()[0].State())
	assert.Equal(t, domainName, allocate.Grants()[0].Shard().Domain)
	assert.Equal(t, model.Regional, revoke.Grants()[0].Shard().Type)
}

func TestCoordinator_CapacityLimitConsumer(t *testing.T) {
	ctx := context.Background()
	cl := mockclock.NewUnsynchronized()

	domain, err := model.NewDomain(domainName, model.Regional, cl.Now(), model.WithDomainConfig(
		model.NewDomainConfig(
			model.WithDomainShardingPolicy(model.NewShardingPolicy(4)),
			model.WithDomainRegions("centralus"),
		),
	))
	require.NoError(t, err)

	coord := setup(ctx, cl, t, []model.Domain{domain}, true)

	w := model.NewInstance(location.NewInstance(location.New("centralus", "pod1")), "endpoint")
	in := make(chan model.ConsumerMessage, 1)
	in <- model.NewRegister(w, serviceName, nil, nil, model.WithCapacityLimit(1))

	out, err := coord.Connect(ctx, session.NewID(), location.NewInstance(location.New("centralus", "splitter1")), in)
	require.NoError(t, err, "consumer failed to join leader")

	snapshot := readFn(t, out, isClusterSnapshot)
	assert.Len(t, snapshot.Assignments(), 0)

	assign := readFn(t, out, isAssign)
	assert.Len(t, assign.Grants(), 1)

	change := readFn(t, out, isClusterChange)
	assert.Len(t, change.Assign().Assignments(), 1)

	in <- model.NewDeregister()

	revoke := readFn(t, out, isRevoke)
	assert.Len(t, revoke.Grants(), 1)
	assert.Equal(t, revoke.Grants()[0].State(), model.RevokedGrantState)

	change = readFn(t, out, isClusterChange)
	assert.Len(t, change.Update().Grants(), 1)
	assert.Equal(t, change.Update().Grants()[0].State(), model.RevokedGrantState)

	in <- model.NewReleased(revoke.Grants()[0])

	coord.Close()
	assertx.Closed(t, out)
}

func TestCoordinator_NamedKeyConsumers(t *testing.T) {
	ctx := context.Background()
	cl := mockclock.NewUnsynchronized()

	names := []model.NamedDomainKey{
		{
			Name: "test",
			Key: model.DomainKey{
				Region: "centralus",
				Key:    model.MustParseKey("b188ea31-f889-4ce5-9fc9-77fda8ab5c83"),
			},
		},
		{
			Name: "test2",
			Key: model.DomainKey{
				Region: "northcentralus",
				Key:    model.MustParseKey("00000000-0000-0000-0000-000000000001"),
			},
		},
	}

	domain, err := model.NewDomain(domainName, model.Regional, cl.Now(), model.WithDomainConfig(
		model.NewDomainConfig(
			model.WithDomainShardingPolicy(model.NewShardingPolicy(4)),
			model.WithDomainRegions("centralus", "northcentralus"),
			model.WithDomainNamedKeys(names...),
		),
	))
	require.NoError(t, err)

	coord := setup(ctx, cl, t, []model.Domain{domain}, true)

	// Worker 1 joins for key "test"
	w := model.NewInstance(location.NewInstance(location.New("centralus", "pod1")), "endpoint")
	in := make(chan model.ConsumerMessage, 1)
	in <- model.NewRegister(w, serviceName, nil, nil, model.WithKeyNames(model.DomainKeyName{
		Domain: domainName.Domain,
		Name:   "test",
	}))

	out, err := coord.Connect(ctx, session.NewID(), location.NewInstance(location.New("centralus", "splitter1")), in)
	require.NoError(t, err, "consumer failed to join leader")

	snapshot := readFn(t, out, isClusterSnapshot)
	assert.Len(t, snapshot.Assignments(), 0)

	assign := readFn(t, out, isAssign)
	assert.Len(t, assign.Grants(), 1)

	change := readFn(t, out, isClusterChange)
	assert.Len(t, change.Assign().Assignments(), 1)

	// Worker 2 joins for key "test2"
	w2 := model.NewInstance(location.NewInstance(location.New("centralus", "pod2")), "endpoint2")
	in2 := make(chan model.ConsumerMessage, 1)
	in2 <- model.NewRegister(w2, serviceName, nil, nil, model.WithKeyNames(model.DomainKeyName{
		Domain: domainName.Domain,
		Name:   "test2",
	}))

	out2, err := coord.Connect(ctx, session.NewID(), location.NewInstance(location.New("centralus", "splitter1")), in2)
	require.NoError(t, err, "consumer failed to join leader")

	snapshot = readFn(t, out2, isClusterSnapshot)
	assert.Len(t, snapshot.Assignments(), 1)

	assign = readFn(t, out2, isAssign)
	assert.Len(t, assign.Grants(), 1)

	change = readFn(t, out2, isClusterChange)
	assert.Len(t, change.Assign().Assignments(), 1)

	// Worker 1 deregisters
	in <- model.NewDeregister()

	revoke := readFn(t, out, isRevoke)
	assert.Len(t, revoke.Grants(), 1)
	assert.Equal(t, revoke.Grants()[0].State(), model.RevokedGrantState)

	change = readFn(t, out, isClusterChange)
	assert.Len(t, change.Update().Grants(), 1)
	assert.Equal(t, change.Update().Grants()[0].State(), model.RevokedGrantState)

	in <- model.NewReleased(revoke.Grants()[0])

	// Worker 2 deregisters
	in2 <- model.NewDeregister()

	revoke = readFn(t, out2, isRevoke)
	assert.Len(t, revoke.Grants(), 1)
	assert.Equal(t, revoke.Grants()[0].State(), model.RevokedGrantState)

	change = readFn(t, out2, isClusterChange)
	assert.Len(t, change.Update().Grants(), 1)
	assert.Equal(t, change.Update().Grants()[0].State(), model.RevokedGrantState)

	in2 <- model.NewReleased(revoke.Grants()[0])

	coord.Close()
	assertx.Closed(t, out)
}

func TestCoordinator_RevokeGrant(t *testing.T) {
	ctx := context.Background()
	cl := mockclock.NewUnsynchronized()

	domain, err := model.NewDomain(domainName, model.Global, cl.Now(), model.WithDomainConfig(
		model.NewDomainConfig(model.WithDomainShardingPolicy(model.NewShardingPolicy(2)))))
	require.NoError(t, err)

	coord := setup(ctx, cl, t, []model.Domain{domain}, true)

	w := model.NewInstance(location.NewInstance(location.New("centralus", "pod1")), "endpoint")
	in := make(chan model.ConsumerMessage, 1)
	in <- model.NewRegister(w, serviceName, nil, nil)

	out, err := coord.Connect(ctx, session.NewID(), location.NewInstance(location.New("centralus", "splitter1")), in)
	require.NoError(t, err, "consumer failed to join coordinator")

	snapshot := readFn(t, out, isClusterSnapshot)
	assert.Len(t, snapshot.Assignments(), 0)

	assign := readFn(t, out, isAssign)
	require.Len(t, assign.Grants(), 1)

	assign2 := readFn(t, out, isAssign)
	require.Len(t, assign2.Grants(), 1)

	change := readFn(t, out, isClusterChange)
	assert.Len(t, change.Assign().Assignments(), 1)

	// connect new consumer
	w2 := model.NewInstance(location.NewInstance(location.New("centralus", "pod2")), "endpoint2")
	in2 := make(chan model.ConsumerMessage, 1)
	in2 <- model.NewRegister(w2, serviceName, nil, nil)

	out2, err := coord.Connect(ctx, session.NewID(), location.NewInstance(location.New("centralus", "splitter")), in2)
	require.NoError(t, err, "consumer failed to join coordinator")

	snapshot2 := readFn(t, out2, isClusterSnapshot)
	assert.Len(t, snapshot2.Assignments(), 1)

	change2 := readFn(t, out, isClusterChange)
	assert.Len(t, change2.Assign().Assignments(), 1)

	change3 := readFn(t, out2, isClusterChange)
	assert.Len(t, change3.Assign().Assignments(), 1)

	toRevoke := assign.Grants()[0]

	req := NewHandleCoordinatorOperationRequest(serviceName, &splitterprivatepb.CoordinatorOperationRequest{
		Req: &splitterprivatepb.CoordinatorOperationRequest_RevokeGrants{
			RevokeGrants: &splitterprivatepb.CoordinatorRevokeGrantsRequest{
				Service: serviceName.ToProto(),
				Grants: []*splitterprivatepb.CoordinatorRevokeGrantsRequest_ConsumerGrants{
					{
						Consumer: string(w.ID()),
						Grants:   []string{string(toRevoke.ID())},
					},
				},
			},
		},
	})

	// Should revoke specific grant id and re-assign it to second consumer
	_, err = coord.Handle(ctx, req)
	require.NoError(t, err)

	revoke := readFn(t, out, isRevoke)
	assert.Len(t, revoke.Grants(), 1)
	assert.Equal(t, toRevoke.ID(), revoke.Grants()[0].ID())

	assign3 := readFn(t, out2, isAssign)
	assert.Len(t, assign3.Grants(), 1)
	assert.Equal(t, toRevoke.Shard().To, assign3.Grants()[0].Shard().To)
	assert.Equal(t, toRevoke.Shard().From, assign3.Grants()[0].Shard().From)
}

func TestCoordinator_CustomShards(t *testing.T) {
	ctx := context.Background()
	cl := mockclock.NewUnsynchronized()

	initialShards := []model.ShardingPolicyShard{
		model.NewShardingPolicyShard(model.MustParseKey("00000000-0000-0000-0000-000000000000"), model.MustParseKey("80000000-0000-0000-0000-000000000000"), ""),
		model.NewShardingPolicyShard(model.MustParseKey("80000000-0000-0000-0000-000000000000"), model.MustParseKey("ffffffff-ffff-ffff-ffff-ffffffffffff"), ""),
	}

	sp := model.NewShardingPolicy(2, model.WithShardingPolicyShards(initialShards))
	opts := model.WithDomainConfig(model.NewDomainConfig(model.WithDomainShardingPolicy(sp), model.WithDomainRegions("centralus")))
	domain, err := model.NewDomain(domainName, model.Regional, cl.Now(), opts)

	require.NoError(t, err)

	coord := setup(ctx, cl, t, []model.Domain{domain}, true)

	consumer1 := model.NewInstance(location.NewInstance(location.New("centralus", "pod1")), "endpoint1")
	in1 := make(chan model.ConsumerMessage, 1)
	in1 <- model.NewRegister(consumer1, serviceName, nil, nil)

	out1, err := coord.Connect(ctx, session.NewID(), location.NewInstance(location.New("centralus", "splitter1")), in1)
	require.NoError(t, err, "First consumer failed to connect")

	readFn(t, out1, isClusterSnapshot)

	var receivedGrants []model.Grant
	for i := 0; i < 2; i++ {
		assign := readFn(t, out1, isAssign)
		assert.Len(t, assign.Grants(), 1, "Expected a single grant in each assign message")
		receivedGrants = append(receivedGrants, assign.Grants()[0])
	}

	assert.Equal(t, 2, len(receivedGrants), "Should have received 2 shards")

	consumer2 := model.NewInstance(location.NewInstance(location.New("centralus", "pod2")), "endpoint2")
	in2 := make(chan model.ConsumerMessage, 1)
	in2 <- model.NewRegister(consumer2, serviceName, nil, nil)

	out2, err := coord.Connect(ctx, session.NewID(), location.NewInstance(location.New("centralus", "splitter1")), in2)
	require.NoError(t, err, "Second consumer failed to connect")

	readFn(t, out2, isClusterSnapshot)

	cl.Add(15 * time.Second)
	time.Sleep(50 * time.Millisecond)

	revoke := readFn(t, out1, isRevoke)
	assert.NotNil(t, revoke, "Expected a revoke message")
	assert.Greater(t, len(revoke.Grants()), 0, "Expected at least one grant to be revoked")

	allocate := readFn(t, out2, isAssign)
	assert.NotNil(t, allocate, "Expected an assign message")
	assert.Greater(t, len(allocate.Grants()), 0, "Expected at least one grant to be allocated")

	in1 <- model.NewReleased(revoke.Grants()...)

	promote := readFn(t, out2, isPromote)
	assert.NotNil(t, promote, "Expected a promote message")
	assert.Greater(t, len(promote.Grants()), 0, "Expected at least one grant to be promoted")
	assert.Equal(t, model.ActiveGrantState, promote.Grants()[0].State(), "Expected active state")

	in1 <- model.NewDeregister()
	in2 <- model.NewDeregister()

	coord.Close()
}

func TestCoordinator_RegionSpecificShards(t *testing.T) {
	ctx := context.Background()
	cl := mockclock.NewUnsynchronized()

	customShards := []model.ShardingPolicyShard{
		model.NewShardingPolicyShard(model.MustParseKey("00000000-0000-0000-0000-000000000000"), model.MustParseKey("10000000-0000-0000-0000-000000000000"), "centralus"),
		model.NewShardingPolicyShard(model.MustParseKey("10000000-0000-0000-0000-000000000000"), model.MustParseKey("20000000-0000-0000-0000-000000000000"), "eastus"),
	}

	sp := model.NewShardingPolicy(2, model.WithShardingPolicyShards(customShards))
	opts := model.WithDomainConfig(model.NewDomainConfig(model.WithDomainShardingPolicy(sp), model.WithDomainRegions("centralus", "eastus")))
	domain, err := model.NewDomain(domainName, model.Regional, cl.Now(), opts)

	require.NoError(t, err)

	coord := setup(ctx, cl, t, []model.Domain{domain}, true)

	expectedRegionResults := map[model.Region][]uuidx.Range{
		"centralus": {
			uuidx.MustNewRange(uuid.MustParse("00000000-0000-0000-0000-000000000000"), uuid.MustParse("10000000-0000-0000-0000-000000000000")),
			uuidx.MustNewRange(uuid.MustParse("10000000-0000-0000-0000-000000000000"), uuid.MustParse("80000000-0000-0000-0000-000000000000")),
			uuidx.MustNewRange(uuid.MustParse("80000000-0000-0000-0000-000000000000"), uuid.MustParse("ffffffff-ffff-ffff-ffff-ffffffffffff")),
		},
		"eastus": {
			uuidx.MustNewRange(uuid.MustParse("00000000-0000-0000-0000-000000000000"), uuid.MustParse("10000000-0000-0000-0000-000000000000")),
			uuidx.MustNewRange(uuid.MustParse("10000000-0000-0000-0000-000000000000"), uuid.MustParse("20000000-0000-0000-0000-000000000000")),
			uuidx.MustNewRange(uuid.MustParse("20000000-0000-0000-0000-000000000000"), uuid.MustParse("80000000-0000-0000-0000-000000000000")),
			uuidx.MustNewRange(uuid.MustParse("80000000-0000-0000-0000-000000000000"), uuid.MustParse("ffffffff-ffff-ffff-ffff-ffffffffffff"))},
	}

	consumer1 := model.NewInstance(location.NewInstance(location.New("centralus", "pod1")), "endpoint1")
	in1 := make(chan model.ConsumerMessage, 1)
	in1 <- model.NewRegister(consumer1, serviceName, nil, nil)

	out1, err := coord.Connect(ctx, session.NewID(), location.NewInstance(location.New("centralus", "splitter1")), in1)
	require.NoError(t, err, "Consumer from centralus failed to connect")

	readFn(t, out1, isClusterSnapshot)

	var allGrants []model.Grant
	for i := 0; i < 7; i++ {
		assign := readFn(t, out1, isAssign)
		allGrants = append(allGrants, assign.Grants()[0])
	}

	consumer2 := model.NewInstance(location.NewInstance(location.New("eastus", "pod1")), "endpoint1")
	in2 := make(chan model.ConsumerMessage, 1)
	in2 <- model.NewRegister(consumer2, serviceName, nil, nil)

	out2, err := coord.Connect(ctx, session.NewID(), location.NewInstance(location.New("eastus", "splitter1")), in2)
	require.NoError(t, err, "Consumer from eastus failed to connect")

	readFn(t, out2, isClusterSnapshot)

	cl.Add(45 * time.Second)
	time.Sleep(1000 * time.Millisecond)

	var eastusGrants []model.Grant
	for i := 0; i < 4; i++ {
		assign := readFn(t, out2, isAssign)
		eastusGrants = append(eastusGrants, assign.Grants()[0])
	}

	assert.True(t, matchesRange(allGrants, expectedRegionResults["centralus"], "centralus"), "Expected centralus ranges not found in allGrants")
	assert.True(t, matchesRange(eastusGrants, expectedRegionResults["eastus"], "eastus"), "Expected eastus ranges not found in eastusGrants")

	in1 <- model.NewDeregister()
	in2 <- model.NewDeregister()
	coord.Close()
}

func matchesRange(grants []model.Grant, ranges []uuidx.Range, region string) bool {
	return slices.ContainsFunc(grants, func(grant model.Grant) bool {
		return slices.ContainsFunc(ranges, func(r uuidx.Range) bool {
			return uuidx.Equal(r.From(), uuid.UUID(grant.Shard().From)) &&
				uuidx.Equal(r.To(), uuid.UUID(grant.Shard().To)) &&
				grant.Shard().Region == model.Region(region)
		})
	})
}

func setup(ctx context.Context, cl clock.Clock, t *testing.T, domains []model.Domain, withFastActivation bool) Coordinator {
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

	var cOpts []Option
	if withFastActivation {
		cOpts = append(cOpts, WithFastActivation())
	}
	c := New(ctx, cl, loc, serviceName, state, updates, cOpts...)
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

func isClusterSnapshot(msg model.ConsumerMessage) (model.ClusterSnapshot, bool) {
	if msg.IsClusterMessage() {
		c, _ := msg.ClusterMessage()
		return c.Snapshot()
	}
	return model.ClusterSnapshot{}, false
}

func isClusterChange(msg model.ConsumerMessage) (model.ClusterChange, bool) {
	if msg.IsClusterMessage() {
		c, _ := msg.ClusterMessage()
		return c.Change()
	}
	return model.ClusterChange{}, false
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
