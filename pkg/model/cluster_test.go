package model_test

import (
	"go.atoms.co/lib/testing/requirex"
	"go.atoms.co/lib/mapx"
	"go.atoms.co/slicex"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/testing/prefab"
	"github.com/stretchr/testify/require"
	"sort"
	"testing"
)

func TestCluster_Owner(t *testing.T) {
	t.Run("key in existing shard", func(t *testing.T) {
		c := setup(t)
		requireOwner(t, c, prefab.NewQDK("t1/s1/d1", "northcentralus", "34345"), prefab.Instance1, model.ActiveGrant)
	})

	t.Run("key outside of existing shard", func(t *testing.T) {
		c := setup(t)
		requireNoOwner(t, c, prefab.NewQDK("t1/s1/d1", "northcentralus", "E4345"))
	})

	t.Run("non-existing domain", func(t *testing.T) {
		c := setup(t)
		requireNoOwner(t, c, prefab.NewQDK("tenant1/service1/domain2", "northcentralus", "B4345"))
	})

	t.Run("non-existing region", func(t *testing.T) {
		c := setup(t)
		requireNoOwner(t, c, prefab.NewQDK("t1/s1/d1", "eastus2", "34345"))
	})
}

func TestCluster_OwnerWithState(t *testing.T) {
	t.Run("key in existing shard with matching state", func(t *testing.T) {
		c := setup(t)
		i, ok := c.OwnerWithState(prefab.NewQDK("t1/s1/d1", "northcentralus", "34345"), model.ActiveGrant)
		require.True(t, ok)
		require.Equal(t, i, prefab.Instance1)

		i, ok = c.OwnerWithState(prefab.NewQDK("t1/s1/d1", "eastus1", "E34345"), model.AllocatedGrant)
		require.True(t, ok)
		require.Equal(t, i, prefab.Instance1)

		i, ok = c.OwnerWithState(prefab.NewQDK("t1/s1/d1", "eastus1", "E34345"), model.RevokedGrant)
		require.True(t, ok)
		require.Equal(t, i, prefab.Instance2)
	})

	t.Run("key in existing shard with mismatching state", func(t *testing.T) {
		c := setup(t)
		_, ok := c.OwnerWithState(prefab.NewQDK("t1/s1/d1", "northcentralus", "34345"), model.AllocatedGrant)
		require.False(t, ok)
	})

	t.Run("key outside of existing shard", func(t *testing.T) {
		c := setup(t)
		_, ok := c.OwnerWithState(prefab.NewQDK("t1/s1/d1", "northcentralus", "E4345"), model.AllocatedGrant)
		require.False(t, ok)
	})

	t.Run("non-existing domain", func(t *testing.T) {
		c := setup(t)
		_, ok := c.OwnerWithState(prefab.NewQDK("tenant1/service1/domain2", "northcentralus", "B4345"), model.AllocatedGrant)
		require.False(t, ok)
	})

	t.Run("non-existing region", func(t *testing.T) {
		c := setup(t)
		_, ok := c.OwnerWithState(prefab.NewQDK("t1/s1/d1", "eastus2", "34345"), model.AllocatedGrant)
		require.False(t, ok)
	})
}

func TestCluster_Consumer(t *testing.T) {
	t.Run("existing consumer", func(t *testing.T) {
		c := setup(t)
		i, ok := c.Consumer(prefab.Instance1.ID())
		require.True(t, ok)
		require.Equal(t, i, prefab.Instance1)
	})

	t.Run("non-existing consumer", func(t *testing.T) {
		c := setup(t)
		_, ok := c.Consumer("id4")
		require.False(t, ok)
	})
}

func TestCluster_Consumers(t *testing.T) {
	c, err := model.NewCluster(nil, nil)
	require.NoError(t, err)
	require.Empty(t, c.Consumers())

	c = setup(t)
	requireConsumers(t, c, prefab.Instance1, prefab.Instance2, prefab.Instance3)
}

func TestCluster_Grant(t *testing.T) {
	t.Run("non-existing grant", func(t *testing.T) {
		c := setup(t)
		_, ok := c.Grant("non-existing")
		require.False(t, ok)
	})

	t.Run("existing grant", func(t *testing.T) {
		c := setup(t)
		g, ok := c.Grant("g11")
		require.True(t, ok)
		expected := prefab.NewGrantInfo("g11", "t1/s1/d1", "northcentralus", "0", "A", model.ActiveGrant)
		requirex.EqualProtobuf(t, g.ToProto(), expected.ToProto())
	})
}

func TestCluster_Grants(t *testing.T) {
	t.Run("existing consumer with shards", func(t *testing.T) {
		c := setup(t)
		grants := c.Grants(prefab.Instance1.ID())
		g11, ok := c.Grant("g11")
		require.True(t, ok)
		g12, ok := c.Grant("g12")
		require.True(t, ok)
		g13, ok := c.Grant("g13")
		require.True(t, ok)
		requireGrantsEqual(t, grants, slicex.New(g11, g12, g13))
	})

	t.Run("existing consumer without shards", func(t *testing.T) {
		c := setup(t)
		grants := c.Grants(prefab.Instance3.ID())
		require.Empty(t, grants)
	})

	t.Run("non-existing consumer", func(t *testing.T) {
		c := setup(t)
		grants := c.Grants(prefab.Instance4.ID())
		require.Empty(t, grants)
	})
}

func TestCluster_GrantConsumer(t *testing.T) {
	t.Run("non-existing grant", func(t *testing.T) {
		c := setup(t)
		_, ok := c.GrantConsumer("non-existing")
		require.False(t, ok)
	})

	t.Run("existing grant", func(t *testing.T) {
		c := setup(t)
		consumer, ok := c.GrantConsumer("g11")
		require.True(t, ok)
		requirex.Equal(t, consumer, prefab.Instance1)
	})
}

func TestCluster_Assign(t *testing.T) {
	t.Run("unknown consumer", func(t *testing.T) {
		c := setup(t)
		g := prefab.NewGrantInfo("g", "t1/s1/d1", "northcentralus", "D", "F", model.ActiveGrant)

		err := c.Assign(slicex.New(prefab.Instance1), map[model.ConsumerID][]model.GrantInfo{prefab.Instance4.ID(): slicex.New(g)})
		require.Error(t, err, "consumer information is missing in assignments: id4")
	})

	t.Run("new consumer without grants", func(t *testing.T) {
		c := setup(t)
		err := c.Assign(slicex.New(prefab.Instance4), map[model.ConsumerID][]model.GrantInfo{})
		require.NoError(t, err)

		// Check new consumer
		require.Empty(t, c.Grants(prefab.Instance4.ID()))
		consumer, ok := c.Consumer(prefab.Instance4.ID())
		require.True(t, ok)
		require.Equal(t, consumer, prefab.Instance4)

		requireConsumers(t, c, prefab.Instance1, prefab.Instance2, prefab.Instance3, prefab.Instance4)

		// Check that existing consumers are not affected
		requireOwner(t, c, prefab.NewQDK("t1/s1/d1", "northcentralus", "34345"), prefab.Instance1, model.ActiveGrant)
		requireOwner(t, c, prefab.NewQDK("t1/s1/d1", "centralus", "34345"), prefab.Instance2, model.ActiveGrant)
	})

	t.Run("new consumer with grants", func(t *testing.T) {
		c := setup(t)
		g := prefab.NewGrantInfo("g", "t1/s1/d1", "northcentralus", "D", "F", model.ActiveGrant)

		err := c.Assign(slicex.New(prefab.Instance4), map[model.ConsumerID][]model.GrantInfo{prefab.Instance4.ID(): slicex.New(g)})
		require.NoError(t, err)

		// Check new consumer
		requireGrantsEqual(t, c.Grants(prefab.Instance4.ID()), slicex.New(g))
		consumer, ok := c.Consumer(prefab.Instance4.ID())
		require.True(t, ok)
		require.Equal(t, consumer, prefab.Instance4)
		consumer, ok = c.GrantConsumer("g")
		require.True(t, ok)
		require.Equal(t, consumer, prefab.Instance4)

		requireConsumers(t, c, prefab.Instance1, prefab.Instance2, prefab.Instance3, prefab.Instance4)
		requireOwner(t, c, prefab.NewQDK("t1/s1/d1", "northcentralus", "ECD"), prefab.Instance4, model.ActiveGrant)

		// Check that existing consumers are not affected
		requireOwner(t, c, prefab.NewQDK("t1/s1/d1", "northcentralus", "34345"), prefab.Instance1, model.ActiveGrant)
		requireOwner(t, c, prefab.NewQDK("t1/s1/d1", "centralus", "34345"), prefab.Instance2, model.ActiveGrant)
	})

	t.Run("existing consumer with new grants", func(t *testing.T) {
		c := setup(t)
		g := prefab.NewGrantInfo("g", "t1/s1/d1", "centralus", "B", "C", model.ActiveGrant)
		g21, ok := c.Grant("g21")
		require.True(t, ok)
		g22, ok := c.Grant("g22")
		require.True(t, ok)

		err := c.Assign(slicex.New(prefab.Instance2), map[model.ConsumerID][]model.GrantInfo{prefab.Instance2.ID(): slicex.New(g)})
		require.NoError(t, err)

		// Check updated consumer
		requireConsumers(t, c, prefab.Instance1, prefab.Instance2, prefab.Instance3)
		requireGrantsEqual(t, c.Grants(prefab.Instance2.ID()), slicex.New(g21, g22, g))
		requireOwner(t, c, prefab.NewQDK("t1/s1/d1", "centralus", "34345"), prefab.Instance2, model.ActiveGrant)
		requireOwner(t, c, prefab.NewQDK("t1/s1/d1", "centralus", "BCD"), prefab.Instance2, model.ActiveGrant)
		requireOwner(t, c, prefab.NewQDK("t1/s1/d1", "eastus1", "ECD"), prefab.Instance2, model.RevokedGrant)

		consumer, ok := c.GrantConsumer("g")
		require.True(t, ok)
		require.Equal(t, consumer, prefab.Instance2)

		// Check that existing consumers are not affected
		requireOwner(t, c, prefab.NewQDK("t1/s1/d1", "northcentralus", "34345"), prefab.Instance1, model.ActiveGrant)
	})

	t.Run("active removed revoked", func(t *testing.T) {
		c := setup(t)
		g13 := prefab.NewGrantInfo("g13", "t1/s1/d1", "eastus1", "E", "F", model.ActiveGrant)

		i, ok := c.OwnerWithState(prefab.NewQDK("t1/s1/d1", "eastus1", "E34345"), model.RevokedGrant)
		require.True(t, ok)
		require.Equal(t, i, prefab.Instance2)

		err := c.Assign(slicex.New(prefab.Instance1), map[model.ConsumerID][]model.GrantInfo{prefab.Instance1.ID(): slicex.New(g13)})
		require.NoError(t, err)

		requireOwner(t, c, prefab.NewQDK("t1/s1/d1", "eastus1", "E34345"), prefab.Instance1, model.ActiveGrant)

		_, ok = c.OwnerWithState(prefab.NewQDK("t1/s1/d1", "eastus1", "E34345"), model.RevokedGrant)
		require.False(t, ok)
	})

	t.Run("grant moved to new consumer", func(t *testing.T) {
		c := setup(t)
		g12 := prefab.NewGrantInfo("g12", "t1/s1/d1", "northcentralus", "A", "D", model.ActiveGrant)

		requireOwner(t, c, prefab.NewQDK("t1/s1/d1", "northcentralus", "A34345"), prefab.Instance1, model.ActiveGrant)

		err := c.Assign(slicex.New(prefab.Instance2), map[model.ConsumerID][]model.GrantInfo{prefab.Instance2.ID(): slicex.New(g12)})
		require.NoError(t, err)

		requireOwner(t, c, prefab.NewQDK("t1/s1/d1", "northcentralus", "A34345"), prefab.Instance2, model.ActiveGrant)
	})
}

func TestCluster_Update(t *testing.T) {
	t.Run("unknown grant", func(t *testing.T) {
		c := setup(t)

		err := c.Update(prefab.NewGrantInfo("g", "t1/s1/d1", "northcentralus", "0", "A", model.ActiveGrant))
		require.Errorf(t, err, "unknown grant: g")
	})

	t.Run("activate grant", func(t *testing.T) {
		c := setup(t)

		i, ok := c.OwnerWithState(prefab.NewQDK("t1/s1/d1", "eastus1", "E34345"), model.RevokedGrant)
		require.True(t, ok)
		require.Equal(t, i, prefab.Instance2)

		g13 := prefab.NewGrantInfo("g13", "t1/s1/d1", "eastus1", "E", "F", model.ActiveGrant)
		err := c.Update(g13)
		require.NoError(t, err)

		requireOwner(t, c, prefab.NewQDK("t1/s1/d1", "eastus1", "E34345"), prefab.Instance1, model.ActiveGrant)

		_, ok = c.OwnerWithState(prefab.NewQDK("t1/s1/d1", "eastus1", "E34345"), model.RevokedGrant)
	})
}

func TestCluster_Unassign(t *testing.T) {
	t.Run("unknown grant", func(t *testing.T) {
		c := setup(t)

		err := c.Unassign("g")
		require.Errorf(t, err, "unknown grant: g")
	})

	t.Run("known grant", func(t *testing.T) {
		c := setup(t)

		_, ok := c.Grant("g13")
		require.True(t, ok)

		err := c.Unassign("g13")

		require.NoError(t, err)
		_, ok = c.Grant("g13")
		require.False(t, ok)
		_, ok = c.GrantConsumer("g13")
		require.False(t, ok)
		g11, ok := c.Grant("g11")
		require.True(t, ok)
		g12, ok := c.Grant("g12")
		require.True(t, ok)
		requireGrantsEqual(t, c.Grants(prefab.Instance1.ID()), slicex.New(g11, g12))
	})
}

func TestCluster_Detach(t *testing.T) {
	t.Run("non existing consumer", func(t *testing.T) {
		c := setup(t)

		err := c.Detach(prefab.Instance4.ID())
		require.Errorf(t, err, "unknown consumer: id4")

		// Check that existing consumers are not affected
		requireConsumers(t, c, prefab.Instance1, prefab.Instance2, prefab.Instance3)
		requireOwner(t, c, prefab.NewQDK("t1/s1/d1", "northcentralus", "34345"), prefab.Instance1, model.ActiveGrant)
		requireOwner(t, c, prefab.NewQDK("t1/s1/d1", "centralus", "34345"), prefab.Instance2, model.ActiveGrant)
	})

	t.Run("existing consumer", func(t *testing.T) {
		c := setup(t)

		err := c.Detach(prefab.Instance1.ID())
		require.NoError(t, err)

		// Check consumer was removed
		requireConsumers(t, c, prefab.Instance2, prefab.Instance3)
		requireNoOwner(t, c, prefab.NewQDK("t1/s1/d1", "northcentralus", "34345"))
		requireNoOwner(t, c, prefab.NewQDK("t1/s1/d1", "northcentralus", "BCD"))

		_, ok := c.Grant("g11")
		require.False(t, ok)
		_, ok = c.GrantConsumer("g11")
		require.False(t, ok)
		require.Empty(t, c.Grants(prefab.Instance1.ID()))

		// Check that existing consumers are not affected
		requireOwner(t, c, prefab.NewQDK("t1/s1/d1", "centralus", "34345"), prefab.Instance2, model.ActiveGrant)
	})
}

func setup(t *testing.T) model.Cluster {
	t.Helper()

	g11 := prefab.NewGrantInfo("g11", "t1/s1/d1", "northcentralus", "0", "A", model.ActiveGrant)
	g12 := prefab.NewGrantInfo("g12", "t1/s1/d1", "northcentralus", "A", "D", model.ActiveGrant)
	g13 := prefab.NewGrantInfo("g13", "t1/s1/d1", "eastus1", "E", "F", model.AllocatedGrant)
	g21 := prefab.NewGrantInfo("g21", "t1/s1/d1", "centralus", "0", "A", model.ActiveGrant)
	g22 := prefab.NewGrantInfo("g22", "t1/s1/d1", "eastus1", "E", "F", model.RevokedGrant)
	grants := map[model.ConsumerID][]model.GrantInfo{
		prefab.Instance1.ID(): slicex.New(g11, g12, g13),
		prefab.Instance2.ID(): slicex.New(g21, g22),
		prefab.Instance3.ID(): slicex.New[model.GrantInfo](),
	}
	c, err := model.NewCluster(slicex.New(prefab.Instance1, prefab.Instance2, prefab.Instance3), grants)
	require.NoError(t, err)

	return c
}

func requireGrantsEqual(t *testing.T, actual []model.GrantInfo, expected []model.GrantInfo) {
	t.Helper()

	if len(actual) != len(expected) {
		require.FailNow(t, "grants are not equal", "%v != %v", len(actual), len(expected))
	}

	actual = slicex.Clone(actual)
	sort.Slice(actual, func(i, j int) bool {
		return actual[i].ID < actual[j].ID
	})
	expected = slicex.Clone(expected)
	sort.Slice(expected, func(i, j int) bool {
		return expected[i].ID < expected[j].ID
	})

	for i := range actual {
		requirex.EqualProtobuf(t, actual[i].ToProto(), expected[i].ToProto())
	}
}

func requireOwner(t *testing.T, c model.Cluster, key model.QualifiedDomainKey, consumer model.Consumer, state model.GrantState) {
	t.Helper()

	i, s, ok := c.Owner(key)
	require.True(t, ok)
	require.Equal(t, i, consumer)
	require.Equal(t, s, state)
}

func requireNoOwner(t *testing.T, c model.Cluster, key model.QualifiedDomainKey) {
	t.Helper()
	_, _, ok := c.Owner(key)
	require.False(t, ok)
}

func requireConsumers(t *testing.T, c model.Cluster, consumers ...model.Consumer) {
	t.Helper()
	require.True(t, mapx.Equals(mapx.New(c.Consumers(), model.Instance.ID), mapx.New(consumers, model.Instance.ID)))
}
