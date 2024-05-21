package model_test

import (
	"go.atoms.co/lib/testing/assertx"
	"go.atoms.co/lib/mapx"
	"go.atoms.co/slicex"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/testing/prefab"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sort"
	"testing"
	"time"
)

func TestCluster(t *testing.T) {
	g1 := prefab.NewGrantInfo("g1", "t/s/d", model.Global, "", "0", "a", model.ActiveGrantState)
	g2 := prefab.NewGrantInfo("g2", "t/s/d", model.Global, "", "a", "d", model.ActiveGrantState)
	g3 := prefab.NewGrantInfo("g3", "t/s/d", model.Global, "", "d", "e", model.RevokedGrantState)
	g4 := prefab.NewGrantInfo("g4", "t/s/d", model.Global, "", "d", "e", model.AllocatedGrantState)

	id := model.ClusterID{Origin: prefab.Instance1.Instance(), Version: 1}

	t.Run("empty", func(t *testing.T) {
		cluster := model.NewClusterMap(id)

		assertx.Equal(t, cluster.ID(), id)
		assert.Len(t, cluster.Consumers(), 0)
		assert.Len(t, cluster.Assignments(), 0)
	})

	t.Run("snapshot", func(t *testing.T) {
		fresh := model.NewClusterMap(id,
			model.NewAssignment(prefab.Instance1, g1, g2),
			model.NewAssignment(prefab.Instance2),
		)
		clone := model.NewClusterMap(id, fresh.Assignments()...)

		for _, cluster := range []*model.ClusterMap{fresh, clone} {
			// (1) Verify consumers

			assert.Len(t, cluster.Consumers(), 2)

			c1, list, ok := cluster.Consumer(prefab.Instance1.ID())
			assert.True(t, ok)
			assertx.Equal(t, c1, prefab.Instance1)
			assert.Len(t, list, 2)

			grants := mapx.New(list, model.GrantInfo.ID)
			assertx.Equal(t, grants[g1.ID()], g1)
			assertx.Equal(t, grants[g2.ID()], g2)

			c2, list, ok := cluster.Consumer(prefab.Instance2.ID())
			assert.True(t, ok)
			assertx.Equal(t, c2, prefab.Instance2)
			assert.Len(t, list, 0)

			_, _, ok = cluster.Consumer(prefab.Instance3.ID())
			assert.False(t, ok)

			// (2) Verify grants

			c, g, ok := cluster.Grant(g1.ID())
			assert.True(t, ok)
			assertx.Equal(t, c, c1)
			assertx.Equal(t, g, g1)

			c, g, ok = cluster.Grant(g2.ID())
			assert.True(t, ok)
			assertx.Equal(t, c, c1)
			assertx.Equal(t, g, g2)

			_, _, ok = cluster.Grant("g3")
			assert.False(t, ok)
		}
	})

	t.Run("update", func(t *testing.T) {
		initial := model.NewClusterMap(id,
			model.NewAssignment(prefab.Instance1, g1, g2),
			model.NewAssignment(prefab.Instance2),
		)

		now := time.Now()

		// (1) Assign grant to new and existing consumer

		assignments := []model.Assignment{
			model.NewAssignment(prefab.Instance2, g3),
			model.NewAssignment(prefab.Instance3, g4),
		}
		cluster, err := model.UpdateClusterMap(initial, model.NewClusterChange(id.Next(now), assignments, nil, nil, nil))
		assert.NoError(t, err)
		assert.Len(t, cluster.Consumers(), 3)

		for _, g := range []model.GrantInfo{g1, g2, g3, g4} {
			_, _, ok := cluster.Grant(g.ID())
			assert.True(t, ok)
		}

		// (2) Update existing grant

		update := []model.GrantInfo{
			model.NewGrantInfo(g1.ID(), g1.Shard(), model.RevokedGrantState),
		}
		cluster, err = model.UpdateClusterMap(initial, model.NewClusterChange(id.Next(now), nil, update, nil, nil))
		assert.NoError(t, err)
		assert.Len(t, cluster.Consumers(), 2)

		_, g, ok := cluster.Grant(g1.ID())
		assert.True(t, ok)
		assertx.Equal(t, g.State(), model.RevokedGrantState)

		// (3) Remove grant

		unassign := []model.GrantID{
			g2.ID(),
		}
		cluster, err = model.UpdateClusterMap(initial, model.NewClusterChange(id.Next(now), nil, nil, unassign, nil))
		assert.NoError(t, err)
		assert.Len(t, cluster.Consumers(), 2)

		_, _, ok = cluster.Grant(g2.ID())
		assert.False(t, ok)

		// (4) Remove consumer, incl any grants

		remove := []model.ConsumerID{
			prefab.Instance1.ID(),
		}
		cluster, err = model.UpdateClusterMap(initial, model.NewClusterChange(id.Next(now), nil, nil, nil, remove))
		assert.NoError(t, err)
		assert.Len(t, cluster.Consumers(), 1)

		_, _, ok = cluster.Consumer(prefab.Instance1.ID())
		assert.False(t, ok)
		_, _, ok = cluster.Consumer(prefab.Instance2.ID())
		assert.True(t, ok)

		_, _, ok = cluster.Grant(g1.ID())
		assert.False(t, ok)
		_, _, ok = cluster.Grant(g2.ID())
		assert.False(t, ok)
	})

	t.Run("lookup", func(t *testing.T) {
		cluster := model.NewClusterMap(id,
			model.NewAssignment(prefab.Instance1, g1, g2, g3),
			model.NewAssignment(prefab.Instance2, g4),
		)

		// (1) Find grants with default [Active, Revoked, Loaded, Unloaded] search.

		c, g, ok := cluster.Lookup(prefab.NewQDK("t/s/d", "", "1"))
		assert.True(t, ok)
		assertx.Equal(t, c, prefab.Instance1)
		assertx.Equal(t, g, g1)

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
		assertx.Equal(t, g, g2)

		// (2) For transitional shards, either leg can be looked up.

		c, g, ok = cluster.Lookup(prefab.NewQDK("t/s/d", "", "d"))
		assert.True(t, ok)
		assertx.Equal(t, c, prefab.Instance1)
		assertx.Equal(t, g, g3) // revoked

		c, g, ok = cluster.Lookup(prefab.NewQDK("t/s/d", "", "d"), model.AllocatedGrantState, model.RevokedGrantState)
		assert.True(t, ok)
		assertx.Equal(t, c, prefab.Instance2)
		assertx.Equal(t, g, g4) // allocated

		_, _, ok = cluster.Lookup(prefab.NewQDK("t/s/d", "", "d"), model.ActiveGrantState)
		assert.False(t, ok)
	})

	t.Run("domain shards", func(t *testing.T) {
		g21 := prefab.NewGrantInfo("g1", "t/s/d2", model.Global, "", "0", "a", model.ActiveGrantState)
		g22 := prefab.NewGrantInfo("g2", "t/s/d2", model.Global, "", "a", "d", model.ActiveGrantState)
		cluster := model.NewClusterMap(id,
			model.NewAssignment(prefab.Instance1, g1, g2),
			model.NewAssignment(prefab.Instance2, g21, g22),
		)

		actual := model.DomainShards(cluster, prefab.QDN("t/s/d"))
		sort.Slice(actual, func(i, j int) bool {
			return actual[i].From.Less(actual[j].From)
		})
		assertx.Equal(t, actual, slicex.New(g1.Shard(), g2.Shard()))

		actual = model.DomainShards(cluster, prefab.QDN("t/s/d2"))
		sort.Slice(actual, func(i, j int) bool {
			return actual[i].From.Less(actual[j].From)
		})
		assertx.Equal(t, actual, slicex.New(g21.Shard(), g22.Shard()))

		require.Empty(t, model.DomainShards(cluster, prefab.QDN("t/s/d3")))
	})
}
