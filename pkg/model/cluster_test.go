package model_test

import (
	"context"
	"go.atoms.co/lib/testing/assertx"
	"go.atoms.co/lib/testing/requirex"
	"go.atoms.co/slicex"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/testing/prefab"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sort"
	"testing"
	"time"
)

var (
	g0A     = prefab.NewGrantInfo("g0A", "t/s/d", model.Global, "", "0", "a", model.ActiveGrantState)
	gAD     = prefab.NewGrantInfo("gAD", "t/s/d", model.Global, "", "a", "d", model.ActiveGrantState)
	gDEAlc  = prefab.NewGrantInfo("gDEAlc", "t/s/d", model.Global, "", "d", "e", model.AllocatedGrantState)
	gDERev  = prefab.NewGrantInfo("gDERev", "t/s/d", model.Global, "", "d", "e", model.RevokedGrantState)
	shard0A = g0A.Shard()
	shardAD = gAD.Shard()
	shardDE = gDEAlc.Shard()

	id = model.ClusterID{Origin: prefab.Instance1.Instance(), Version: 1}
)

func TestCluster(t *testing.T) {
	ctx := context.Background()

	t.Run("empty", func(t *testing.T) {
		cluster := model.NewClusterMap(id, nil)

		assertx.Equal(t, cluster.ID(), id)
		assert.Len(t, cluster.Consumers(), 0)
		assert.Len(t, cluster.Assignments(), 0)
		assert.Len(t, cluster.Shards(), 0)

		cluster = model.NewClusterMap(id, slicex.New(shard0A, shardAD))
		assert.Len(t, cluster.Shards(), 2)
		assertShardGrantsEmpty(t, cluster, shard0A)
		assertShardGrantsEmpty(t, cluster, shardAD)
	})

	t.Run("assignments", func(t *testing.T) {
		fresh := newCluster(t, slicex.New(model.NewAssignment(prefab.Instance1, g0A, gAD), model.NewAssignment(prefab.Instance2)))
		clone := newCluster(t, fresh.Assignments())

		for _, cluster := range []*model.ClusterMap{fresh, clone} {
			// (1) Verify consumers

			assert.Len(t, cluster.Consumers(), 2)
			assertConsumerGrants(t, cluster, prefab.Instance1, g0A, gAD)
			assertConsumerGrantsEmpty(t, cluster, prefab.Instance2)
			assertConsumerNotPresent(t, cluster, prefab.Instance3)

			assertGrantNotFound(t, cluster, gDERev)

			assert.Len(t, cluster.Shards(), 2)
			assertShardGrants(t, cluster, shard0A, g0A)
			assertShardGrants(t, cluster, shardAD, gAD)
		}
	})

	t.Run("update", func(t *testing.T) {
		initial := newCluster(t, slicex.New(model.NewAssignment(prefab.Instance1, g0A), model.NewAssignment(prefab.Instance2, gDERev)))

		assert.Len(t, initial.Shards(), 2)
		assertShardGrants(t, initial, shard0A, g0A)
		assertShardGrants(t, initial, gDERev.Shard(), gDERev)

		assert.Len(t, initial.Consumers(), 2)
		assertConsumerGrants(t, initial, prefab.Instance1, g0A)
		assertConsumerGrants(t, initial, prefab.Instance2, gDERev)

		now := time.Now()

		t.Run("assign overwrites old grants", func(t *testing.T) {
			// Assign a new grant with the same shard as an existing grant
			g0A1 := prefab.NewGrantInfo("g0A1", "t/s/d", model.Global, "", "0", "a", model.ActiveGrantState)
			assignments := slicex.New(model.NewAssignment(prefab.Instance2, g0A1))
			cluster, err := model.UpdateClusterMap(ctx, initial, model.NewClusterChange(initial.ID().Next(now), assignments, nil, nil, nil))
			assert.NoError(t, err)

			// The old grant should be removed from the consumer
			assert.Len(t, cluster.Consumers(), 2)
			assertConsumerGrantsEmpty(t, cluster, prefab.Instance1)
			assertConsumerGrants(t, cluster, prefab.Instance2, g0A1, gDERev)

			assertGrantNotFound(t, cluster, g0A)

			// The old grant should be removed from the shard
			assert.Len(t, cluster.Shards(), 2)
			assertShardGrants(t, cluster, shard0A, g0A1)
			assertShardGrants(t, cluster, shardDE, gDERev)

			for _, g := range []model.GrantInfo{g0A1, gDERev} {
				_, _, ok := cluster.Grant(g.ID())
				assert.True(t, ok)
			}

			// Lookup grants should be updated
			assertLookup(t, cluster, prefab.Instance2, g0A1, "1")
			assertLookup(t, cluster, prefab.Instance2, gDERev, "d")
		})

		t.Run("assign new grants", func(t *testing.T) {
			assignments := []model.Assignment{
				model.NewAssignment(prefab.Instance2, gAD),
				model.NewAssignment(prefab.Instance3, gDEAlc),
			}
			cluster, err := model.UpdateClusterMap(ctx, initial, model.NewClusterChange(initial.ID().Next(now), assignments, nil, nil, nil))
			assert.NoError(t, err)

			assert.Len(t, cluster.Consumers(), 3)
			assertConsumerGrants(t, cluster, prefab.Instance1, g0A)
			assertConsumerGrants(t, cluster, prefab.Instance2, gAD, gDERev)
			assertConsumerGrants(t, cluster, prefab.Instance3, gDEAlc)

			assert.Len(t, cluster.Shards(), 3)
			assertShardGrants(t, cluster, shard0A, g0A)
			assertShardGrants(t, cluster, shardAD, gAD)
			assertShardGrants(t, cluster, gDERev.Shard(), gDERev, gDEAlc)

			for _, g := range []model.GrantInfo{g0A, gAD, gDERev, gDEAlc} {
				_, _, ok := cluster.Grant(g.ID())
				assert.True(t, ok)
			}
		})

		t.Run("update existing grant", func(t *testing.T) {
			update := []model.GrantInfo{
				model.NewGrantInfo(g0A.ID(), shard0A, model.RevokedGrantState),
			}
			cluster, err := model.UpdateClusterMap(ctx, initial, model.NewClusterChange(initial.ID().Next(now), nil, update, nil, nil))
			assert.NoError(t, err)
			assert.Len(t, cluster.Consumers(), 2)

			assert.Len(t, cluster.Shards(), 2)
			assertShardGrants(t, cluster, shard0A, g0A)
			assertShardGrants(t, cluster, gDERev.Shard(), gDERev)

			_, g, ok := cluster.Grant(g0A.ID())
			assert.True(t, ok)
			assertx.Equal(t, g.State(), model.RevokedGrantState)
		})

		t.Run("remove existing grant", func(t *testing.T) {
			unassign := []model.GrantID{
				gDERev.ID(),
			}
			cluster, err := model.UpdateClusterMap(ctx, initial, model.NewClusterChange(initial.ID().Next(now), nil, nil, unassign, nil))
			assert.NoError(t, err)
			assert.Len(t, cluster.Consumers(), 2)

			assert.Len(t, cluster.Shards(), 2)
			assertShardGrants(t, cluster, shard0A, g0A)
			assertShardGrantsEmpty(t, cluster, gDERev.Shard())

			_, _, ok := cluster.Grant(gAD.ID())
			assert.False(t, ok)
		})

		t.Run("remove consumer", func(t *testing.T) {
			remove := []model.ConsumerID{
				prefab.Instance1.ID(),
			}
			cluster, err := model.UpdateClusterMap(ctx, initial, model.NewClusterChange(initial.ID().Next(now), nil, nil, nil, remove))
			assert.NoError(t, err)

			assert.Len(t, cluster.Consumers(), 1)
			_, _, ok := cluster.Consumer(prefab.Instance1.ID())
			assert.False(t, ok)
			_, _, ok = cluster.Consumer(prefab.Instance2.ID())
			assert.True(t, ok)

			_, _, ok = cluster.Grant(g0A.ID())
			assert.False(t, ok)
			_, _, ok = cluster.Grant(gAD.ID())
			assert.False(t, ok)

			assert.Len(t, cluster.Shards(), 2)
			assertShardGrantsEmpty(t, cluster, shard0A)
			assertShardGrants(t, cluster, gDERev.Shard(), gDERev)
		})
	})

	t.Run("lookup", func(t *testing.T) {
		cluster := newCluster(t, slicex.New(model.NewAssignment(prefab.Instance1, g0A, gAD, gDERev), model.NewAssignment(prefab.Instance2, gDEAlc)))

		// (1) Find grants with default [Active, Revoked, Loaded, Unloaded] search.

		c, g, ok := cluster.Lookup(prefab.NewQDK("t/s/d", "", "1"))
		assert.True(t, ok)
		assertx.Equal(t, c, prefab.Instance1)
		assertx.Equal(t, g, g0A)

		_, _, ok = cluster.Lookup(prefab.NewQDK("t/s/d", "", "1"), model.ActiveGrantState)
		assert.True(t, ok)
		_, _, ok = cluster.Lookup(prefab.NewQDK("t/s/d", "", "1"), model.RevokedGrantState, model.ActiveGrantState)
		assert.True(t, ok)
		_, _, ok = cluster.Lookup(prefab.NewQDK("t/s/d", "", "1"), model.AllocatedGrantState)
		assert.False(t, ok)
		_, _, ok = cluster.Lookup(prefab.NewQDK("t/s/d", "", "1"), model.RevokedGrantState)
		assert.False(t, ok)

		c, g, ok = cluster.Lookup(prefab.NewQDK("t/s/d", "", "b"))
		assert.True(t, ok)
		assertx.Equal(t, c, prefab.Instance1)
		assertx.Equal(t, g, gAD)

		// (2) For transitional shards, either leg can be looked up.

		c, g, ok = cluster.Lookup(prefab.NewQDK("t/s/d", "", "d"))
		assert.True(t, ok)
		assertx.Equal(t, c, prefab.Instance1)
		assertx.Equal(t, g, gDERev) // revoked

		c, g, ok = cluster.Lookup(prefab.NewQDK("t/s/d", "", "d"), model.AllocatedGrantState, model.RevokedGrantState)
		assert.True(t, ok)
		assertx.Equal(t, c, prefab.Instance2)
		assertx.Equal(t, g, gDEAlc) // allocated

		_, _, ok = cluster.Lookup(prefab.NewQDK("t/s/d", "", "d"), model.ActiveGrantState)
		assert.False(t, ok)
	})

	t.Run("domain shards", func(t *testing.T) {
		g21 := prefab.NewGrantInfo("g21", "t/s/d2", model.Global, "", "0", "a", model.ActiveGrantState)
		g22 := prefab.NewGrantInfo("g22", "t/s/d2", model.Global, "", "a", "d", model.ActiveGrantState)
		cluster := newCluster(t, slicex.New(model.NewAssignment(prefab.Instance1, g0A, gAD), model.NewAssignment(prefab.Instance2, g21, g22)))

		actual := model.DomainShards(cluster, prefab.QDN("t/s/d"))
		sort.Slice(actual, func(i, j int) bool {
			return actual[i].From.Less(actual[j].From)
		})
		assertx.Equal(t, actual, slicex.New(shard0A, shardAD))

		actual = model.DomainShards(cluster, prefab.QDN("t/s/d2"))
		sort.Slice(actual, func(i, j int) bool {
			return actual[i].From.Less(actual[j].From)
		})
		assertx.Equal(t, actual, slicex.New(g21.Shard(), g22.Shard()))

		require.Empty(t, model.DomainShards(cluster, prefab.QDN("t/s/d3")))
	})

	t.Run("empty cluster applies empty snapshot", func(t *testing.T) {
		// Initialize cluster with empty snapshot
		cluster := newCluster(t, nil)

		// Cluster is empty
		assert.Len(t, cluster.Shards(), 0)
		assert.Len(t, cluster.Consumers(), 0)
		assertLookupNotFound(t, cluster, "1")
	})

	t.Run("empty cluster applies snapshot with empty shards and some assignments", func(t *testing.T) {
		// Initialize the cluster with various assignments and empty shards
		assignments := slicex.New(model.NewAssignment(prefab.Instance1, g0A), model.NewAssignment(prefab.Instance2), model.NewAssignment(prefab.Instance3, gAD))
		cluster := newCluster(t, assignments)

		// Shards from assignments should be present
		assert.Len(t, cluster.Shards(), 2)
		assertShardGrants(t, cluster, shard0A, g0A)
		assertShardGrants(t, cluster, shardAD, gAD)

		// Consumers from assignments should be present
		assert.Len(t, cluster.Consumers(), 3)
		assertConsumerGrants(t, cluster, prefab.Instance1, g0A)
		assertConsumerGrantsEmpty(t, cluster, prefab.Instance2)
		assertConsumerGrants(t, cluster, prefab.Instance3, gAD)

		// Lookup grants should include new grants
		assertLookup(t, cluster, prefab.Instance1, g0A, "1")
		assertLookup(t, cluster, prefab.Instance3, gAD, "b")
	})

	t.Run("empty cluster applies snapshot with some shards and empty assignments", func(t *testing.T) {
		// Initialize the cluster with empty assignments and some shards
		cluster := newCluster(t, nil, shard0A, shardAD)

		// Given shards are present
		assert.Len(t, cluster.Shards(), 2)
		assertShardGrantsEmpty(t, cluster, shard0A)
		assertShardGrantsEmpty(t, cluster, shardAD)

		// Cluster is empty, no grants nor consumers
		assert.Len(t, cluster.Consumers(), 0)
		assertLookupNotFound(t, cluster, "1")
	})

	t.Run("empty cluster applies snapshot with some shards and some assignments", func(t *testing.T) {
		// Initialize the cluster with various assignments and some shards
		assignments := slicex.New(model.NewAssignment(prefab.Instance1, g0A), model.NewAssignment(prefab.Instance2), model.NewAssignment(prefab.Instance3, gAD))
		cluster := newCluster(t, assignments, shardDE)

		// Shards should be merged from given shards and shards in the assignments
		assert.Len(t, cluster.Shards(), 3)
		assertShardGrants(t, cluster, shard0A, g0A)
		assertShardGrants(t, cluster, shardAD, gAD)
		assertShardGrantsEmpty(t, cluster, shardDE)

		// Consumers from assignments should be present
		assert.Len(t, cluster.Consumers(), 3)
		assertConsumerGrants(t, cluster, prefab.Instance1, g0A)
		assertConsumerGrantsEmpty(t, cluster, prefab.Instance2)
		assertConsumerGrants(t, cluster, prefab.Instance3, gAD)

		// Lookup grants should include new grants.
		assertLookup(t, cluster, prefab.Instance1, g0A, "1")
		assertLookup(t, cluster, prefab.Instance3, gAD, "b")
		assertLookupNotFound(t, cluster, "d") // Empty registered shard should not contain any grants
	})

	t.Run("cluster applies another snapshot with empty shards and empty assignments", func(t *testing.T) {
		// Initialize the cluster with various assignments and empty shards
		assignments := slicex.New(model.NewAssignment(prefab.Instance1, g0A), model.NewAssignment(prefab.Instance2), model.NewAssignment(prefab.Instance3, gAD))
		cluster := newCluster(t, assignments)

		// Update with empty assignments and empty shards
		cluster = applySnapshot(t, cluster, nil)

		// Shards should be the same
		assert.Len(t, cluster.Shards(), 2)
		assertShardGrants(t, cluster, shard0A, g0A)
		assertShardGrants(t, cluster, shardAD, gAD)

		// Consumer grants should be the same except for the consumer with no grants
		assert.Len(t, cluster.Consumers(), 2)
		assertConsumerGrants(t, cluster, prefab.Instance1, g0A)
		assertConsumerNotPresent(t, cluster, prefab.Instance2) // consumer with no grants is removed
		assertConsumerGrants(t, cluster, prefab.Instance3, gAD)

		// Lookup grants should stay the same
		assertLookup(t, cluster, prefab.Instance1, g0A, "1")
		assertLookup(t, cluster, prefab.Instance3, gAD, "b")
	})

	t.Run("cluster applies another snapshot with empty shards and some assignments", func(t *testing.T) {
		// Initialize the cluster with various assignments and empty shards
		assignments := slicex.New(model.NewAssignment(prefab.Instance1, g0A), model.NewAssignment(prefab.Instance2), model.NewAssignment(prefab.Instance3, gAD))
		cluster := newCluster(t, assignments)

		// Update multiple times
		for range 2 {
			// Update with new assignments and empty shards
			assignments = slicex.New(model.NewAssignment(prefab.Instance1, gDEAlc), model.NewAssignment(prefab.Instance3), model.NewAssignment(prefab.Instance4, gDERev))
			cluster = applySnapshot(t, cluster, assignments)

			// Shards should be created from the new assignments. Old shards should be removed
			assert.Len(t, cluster.Shards(), 1)
			assertShardGrants(t, cluster, shardDE, gDEAlc, gDERev)

			// Consumer grants should be created from the new assignments. Old consumers without grants are removed
			assert.Len(t, cluster.Consumers(), 3)
			assertConsumerGrants(t, cluster, prefab.Instance1, gDEAlc)
			assertConsumerNotPresent(t, cluster, prefab.Instance2) // consumer with no grants is removed
			assertConsumerGrantsEmpty(t, cluster, prefab.Instance3)
			assertConsumerGrants(t, cluster, prefab.Instance4, gDERev)

			// Lookup grants should use the new grants
			assertLookup(t, cluster, prefab.Instance1, gDEAlc, "d", model.AllocatedGrantState)
			assertLookup(t, cluster, prefab.Instance4, gDERev, "d")
			assertLookupNotFound(t, cluster, "1")
			assertLookupNotFound(t, cluster, "a")
		}
	})

	t.Run("cluster applies another snapshot with some shards and empty assignments", func(t *testing.T) {
		// Initialize the cluster with various assignments and empty shards
		assignments := slicex.New(model.NewAssignment(prefab.Instance1, g0A), model.NewAssignment(prefab.Instance2), model.NewAssignment(prefab.Instance3, gAD))
		cluster := newCluster(t, assignments)

		// Update multiple times
		for range 2 {
			// Update with empty assignments and some shards
			cluster = applySnapshot(t, cluster, nil, shard0A, shardDE)

			// Shards should be created from the new assignments. Old shards should be removed
			assert.Len(t, cluster.Shards(), 2)
			assertShardGrants(t, cluster, shard0A, g0A)
			assertShardGrantsEmpty(t, cluster, shardDE)
			assertShardNotPresent(t, cluster, shardAD) // Old shard removed

			// Consumer grants should be created from the old assignments. Old consumers without grants are removed
			assert.Len(t, cluster.Consumers(), 1)
			assertConsumerGrants(t, cluster, prefab.Instance1, g0A)
			assertConsumerNotPresent(t, cluster, prefab.Instance2) // consumer with no grants is removed
			assertConsumerNotPresent(t, cluster, prefab.Instance3) // consumer is gone after removing its grant

			// Lookup grants should use the old grants
			assertLookup(t, cluster, prefab.Instance1, g0A, "0")
			assertLookupNotFound(t, cluster, "a")
		}
	})

	t.Run("cluster applies another snapshot with some shards and some assignments", func(t *testing.T) {
		// Initialize the cluster with various assignments and empty shards
		assignments := slicex.New(model.NewAssignment(prefab.Instance1, g0A), model.NewAssignment(prefab.Instance2), model.NewAssignment(prefab.Instance3, gAD))
		cluster := newCluster(t, assignments)

		// Update multiple times
		for range 2 {
			// Update with new assignments and new shards
			assignments = slicex.New(model.NewAssignment(prefab.Instance1, gDEAlc), model.NewAssignment(prefab.Instance4, gDERev))
			cluster = applySnapshot(t, cluster, assignments, shard0A)

			// Shards should be created from the new shards and combined assignments
			assert.Len(t, cluster.Shards(), 2)
			assertShardGrants(t, cluster, shard0A, g0A) // old grant is kept, shard is not assigned in new assignments
			assertShardGrants(t, cluster, shardDE, gDEAlc, gDERev)
			assertShardNotPresent(t, cluster, shardAD)

			// Consumer grants should be created from combined assignments. Old consumers without grants are removed
			assert.Len(t, cluster.Consumers(), 2)
			assertConsumerGrants(t, cluster, prefab.Instance1, g0A, gDEAlc)
			assertConsumerNotPresent(t, cluster, prefab.Instance2) // consumer with no grants is removed
			assertConsumerNotPresent(t, cluster, prefab.Instance3) // consumer is gone after removing its grant
			assertConsumerGrants(t, cluster, prefab.Instance4, gDERev)

			// Lookup grants should use the combined grants
			assertLookup(t, cluster, prefab.Instance1, gDEAlc, "d", model.AllocatedGrantState)
			assertLookup(t, cluster, prefab.Instance4, gDERev, "d")
			assertLookup(t, cluster, prefab.Instance1, g0A, "1")
			assertLookupNotFound(t, cluster, "a") // old grant is removed
		}
	})

	for _, state := range slicex.New(model.AllocatedGrantState, model.LoadedGrantState) {
		t.Run(fmt.Sprintf("cluster with allocated and revoked grants applies another snapshot with %v grant", state.String()), func(t *testing.T) {
			// Initialize the cluster with allocated and revoked grants
			assignments := slicex.New(model.NewAssignment(prefab.Instance1, gDEAlc), model.NewAssignment(prefab.Instance3, gDERev))
			cluster := newCluster(t, assignments, shardDE)

			// Update with given state (maybe different from the previous update)
			gDEAlc := prefab.NewGrantInfo("gDEAlc", "t/s/d", model.Global, "", "d", "e", state)
			assignments = slicex.New(model.NewAssignment(prefab.Instance2, gDEAlc))
			cluster = applySnapshot(t, cluster, assignments, shardDE)

			// Shards should be created from combined assignments
			assert.Len(t, cluster.Shards(), 1)
			assertShardGrants(t, cluster, shardDE, gDEAlc, gDERev) // both grants are kept

			// Consumer grants should be created from combined assignments. Old consumers without grants are removed
			assert.Len(t, cluster.Consumers(), 2)
			assertConsumerGrants(t, cluster, prefab.Instance2, gDEAlc)
			assertConsumerGrants(t, cluster, prefab.Instance3, gDERev)

			// Lookup grants should use the combined grants
			if state == model.AllocatedGrantState {
				assertLookup(t, cluster, prefab.Instance2, gDEAlc, "d", model.AllocatedGrantState)
			}
			assertLookup(t, cluster, prefab.Instance3, gDERev, "d")
		})
	}

	for _, state := range slicex.New(model.RevokedGrantState, model.UnloadedGrantState) {
		t.Run(fmt.Sprintf("cluster with allocated and revoked grants applies another snapshot with %v grant", state), func(t *testing.T) {
			// Initialize the cluster with allocated and revoked grants
			assignments := slicex.New(model.NewAssignment(prefab.Instance1, gDEAlc), model.NewAssignment(prefab.Instance3, gDERev))
			cluster := newCluster(t, assignments, shardDE)

			// Update grant with given state (maybe different from the previous update)
			gDERev := prefab.NewGrantInfo("gDERev", "t/s/d", model.Global, "", "d", "e", state)
			assignments = slicex.New(model.NewAssignment(prefab.Instance2, gDERev))
			cluster = applySnapshot(t, cluster, assignments, shardDE)

			// Shards should be created from the combined assignments
			assert.Len(t, cluster.Shards(), 1)
			assertShardGrants(t, cluster, shardDE, gDEAlc, gDERev) // both grants are kept

			// Consumer grants should be created from combined assignments. Old consumers without grants are removed
			assert.Len(t, cluster.Consumers(), 2)
			assertConsumerGrants(t, cluster, prefab.Instance1, gDEAlc)
			assertConsumerGrants(t, cluster, prefab.Instance2, gDERev)

			// Lookup grants should use the combined grants
			assertLookup(t, cluster, prefab.Instance1, gDEAlc, "d", model.AllocatedGrantState)
			assertLookup(t, cluster, prefab.Instance2, gDERev, "d")
		})
	}

	t.Run("cluster with allocated and revoked grants applies another snapshot with no assignments", func(t *testing.T) {
		// Initialize the cluster with allocated and revoked grants
		assignments := slicex.New(model.NewAssignment(prefab.Instance1, gDEAlc), model.NewAssignment(prefab.Instance3, gDERev))
		cluster := newCluster(t, assignments, shardDE)

		// Update with an empty snapshot
		cluster = applySnapshot(t, cluster, nil, shardDE)

		// Shards should be created from the old assignments
		assert.Len(t, cluster.Shards(), 1)
		assertShardGrants(t, cluster, shardDE, gDEAlc, gDERev) // both grants are kept

		// Consumer grants should be created from old assignments.
		assert.Len(t, cluster.Consumers(), 2)
		assertConsumerGrants(t, cluster, prefab.Instance1, gDEAlc)
		assertConsumerGrants(t, cluster, prefab.Instance3, gDERev)

		// Lookup grants should use the old grants
		assertLookup(t, cluster, prefab.Instance1, gDEAlc, "d", model.AllocatedGrantState)
		assertLookup(t, cluster, prefab.Instance3, gDERev, "d")
	})

	t.Run("cluster with allocated and revoked grants applies another snapshot with active assignment", func(t *testing.T) {
		// Initialize the cluster with allocated and revoked grants
		assignments := slicex.New(model.NewAssignment(prefab.Instance1, gDEAlc), model.NewAssignment(prefab.Instance3, gDERev))
		cluster := newCluster(t, assignments, shardDE)

		// Update with a new active grant for the same shard
		gDEAct := prefab.NewGrantInfo("gDEAct", "t/s/d", model.Global, "", "d", "e", model.ActiveGrantState)
		assignments = slicex.New(model.NewAssignment(prefab.Instance2, gDEAct))
		cluster = applySnapshot(t, cluster, assignments, shardDE)

		// Shards should be created from the old assignments
		assert.Len(t, cluster.Shards(), 1)
		assertShardGrants(t, cluster, shardDE, gDEAct) // old grants are removed

		// Consumer grants should be created from the new assignment.
		assert.Len(t, cluster.Consumers(), 1)
		assertConsumerGrants(t, cluster, prefab.Instance2, gDEAct)

		// Lookup grants should use the new grant
		assertLookup(t, cluster, prefab.Instance2, gDEAct, "d")
	})

	t.Run("cluster with allocated and revoked grants applies another snapshot with new assignments", func(t *testing.T) {
		// Initialize the cluster with allocated and revoked grants
		assignments := slicex.New(model.NewAssignment(prefab.Instance1, gDEAlc), model.NewAssignment(prefab.Instance3, gDERev))
		cluster := newCluster(t, assignments, shardDE)

		// Update with new grants assigned to the same shard
		gDENewAlc := prefab.NewGrantInfo("gDENewAlc", "t/s/d", model.Global, "", "d", "e", model.AllocatedGrantState)
		gDENewRev := prefab.NewGrantInfo("gDENewRev", "t/s/d", model.Global, "", "d", "e", model.RevokedGrantState)
		assignments = slicex.New(model.NewAssignment(prefab.Instance1, gDENewRev), model.NewAssignment(prefab.Instance2, gDENewAlc))
		cluster = applySnapshot(t, cluster, assignments, shardDE)

		// Shards should be created from the new assignments
		assert.Len(t, cluster.Shards(), 1)
		assertShardGrants(t, cluster, shardDE, gDENewAlc, gDENewRev) // old grants are removed

		// Consumer grants should be created from the new assignment.
		assert.Len(t, cluster.Consumers(), 2)
		assertConsumerGrants(t, cluster, prefab.Instance1, gDENewRev)
		assertConsumerGrants(t, cluster, prefab.Instance2, gDENewAlc)

		// Lookup grants should use the new grants
		assertLookup(t, cluster, prefab.Instance2, gDENewAlc, "d", model.AllocatedGrantState)
		assertLookup(t, cluster, prefab.Instance1, gDENewRev, "d")
	})

	t.Run("cluster with a single grant applies another snapshot with new grant for the same shard", func(t *testing.T) {
		// Initialize the cluster with active grant
		assignments := slicex.New(model.NewAssignment(prefab.Instance1, g0A))
		cluster := newCluster(t, assignments, shard0A)

		// Update with new grant assigned to the same shard
		g0ANew := prefab.NewGrantInfo("g0ANew", "t/s/d", model.Global, "", "0", "a", model.ActiveGrantState)
		assignments = slicex.New(model.NewAssignment(prefab.Instance1, g0ANew))
		cluster = applySnapshot(t, cluster, assignments, shard0A)

		// Shards should be created from the new assignment
		assert.Len(t, cluster.Shards(), 1)
		assertShardGrants(t, cluster, shard0A, g0ANew) // old grant is removed

		// Consumer grants should be created from the new assignment.
		assert.Len(t, cluster.Consumers(), 1)
		assertConsumerGrants(t, cluster, prefab.Instance1, g0ANew)

		// Lookup grants should use the new grant
		assertLookup(t, cluster, prefab.Instance1, g0ANew, "0")
	})
}

func newCluster(t *testing.T, assignments []model.Assignment, shards ...model.Shard) *model.ClusterMap {
	t.Helper()
	cluster := model.NewClusterMap(id, nil)
	return applySnapshot(t, cluster, assignments, shards...)
}

func applySnapshot(t *testing.T, c *model.ClusterMap, assignments []model.Assignment, shards ...model.Shard) *model.ClusterMap {
	t.Helper()
	cluster, err := model.UpdateClusterMap(context.Background(), c, model.NewClusterSnapshot(id.Next(time.Now()), assignments, shards))
	require.NoError(t, err)
	return cluster
}

func assertLookup(t *testing.T, c *model.ClusterMap, consumer model.Instance, info model.GrantInfo, id string, states ...model.GrantState) {
	t.Helper()
	grantOwner, grant, ok := c.Lookup(prefab.NewQDK("t/s/d", "", id), states...)
	assert.True(t, ok)
	requirex.EqualProtobuf(t, model.UnwrapInstance(grantOwner), model.UnwrapInstance(consumer))
	requirex.EqualProtobuf(t, model.UnwrapGrantInfo(grant), model.UnwrapGrantInfo(info))
}

func assertLookupNotFound(t *testing.T, c *model.ClusterMap, id string, states ...model.GrantState) {
	t.Helper()
	_, _, ok := c.Lookup(prefab.NewQDK("t/s/d", "", id), states...)
	assert.False(t, ok)
}

func assertGrantNotFound(t *testing.T, c *model.ClusterMap, info model.GrantInfo) {
	t.Helper()

	_, _, ok := c.Grant(info.ID())
	assert.False(t, ok)
}

func assertConsumerGrants(t *testing.T, c *model.ClusterMap, consumer model.Instance, grants ...model.GrantInfo) {
	t.Helper()
	_, g, ok := c.Consumer(consumer.ID())
	assert.True(t, ok)
	assertx.Equal(t, sortedGrantIDs(slicex.Map(g, model.GrantInfo.ID)), sortedGrantIDs(slicex.Map(grants, model.GrantInfo.ID)))

	for _, g := range grants {
		c, info, ok := c.Grant(g.ID())
		assert.True(t, ok)
		assertx.EqualProtobuf(t, model.UnwrapInstance(c), model.UnwrapInstance(consumer))
		assertx.EqualProtobuf(t, model.UnwrapGrantInfo(info), model.UnwrapGrantInfo(g))
	}
}

func assertConsumerGrantsEmpty(t *testing.T, c *model.ClusterMap, consumer model.Instance) {
	t.Helper()
	_, g, ok := c.Consumer(consumer.ID())
	assert.True(t, ok)
	assert.Empty(t, g)
}

func assertConsumerNotPresent(t *testing.T, c *model.ClusterMap, consumer model.Instance) {
	t.Helper()
	_, _, ok := c.Consumer(consumer.ID())
	assert.False(t, ok)
}

func assertShardGrantsEmpty(t *testing.T, c *model.ClusterMap, shard model.Shard) {
	t.Helper()
	shards, ok := c.ShardGrants(shard)
	assert.True(t, ok)
	assert.Empty(t, shards)
}

func assertShardGrants(t *testing.T, c *model.ClusterMap, shard model.Shard, grants ...model.GrantInfo) {
	t.Helper()
	shards, ok := c.ShardGrants(shard)
	assert.True(t, ok)
	assertx.Equal(t, sortedGrantIDs(shards), sortedGrantIDs(slicex.Map(grants, model.GrantInfo.ID)))
}

func assertShardNotPresent(t *testing.T, c *model.ClusterMap, shard model.Shard) {
	t.Helper()
	_, ok := c.ShardGrants(shard)
	assert.False(t, ok)
}

func sortedGrantIDs(ids []model.GrantID) []model.GrantID {
	sort.Slice(ids, func(i, j int) bool {
		return ids[i] < ids[j]
	})
	return ids
}
