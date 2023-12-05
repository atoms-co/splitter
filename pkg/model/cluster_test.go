package model_test

import (
	"go.atoms.co/lib/testing/requirex"
	"go.atoms.co/lib/mapx"
	"go.atoms.co/slicex"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/testing/prefab"
	"fmt"
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
	c, err := model.NewCluster("", 0, nil, nil)
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
		expected := prefab.NewGrantInfo("g11", "t1/s1/d1", model.Regional, "northcentralus", "0", "A", model.ActiveGrant)
		requirex.EqualProtobuf(t, model.UnwrapGrantInfo(grant(t, c, "g11")), model.UnwrapGrantInfo(expected))
	})
}

func TestCluster_Grants(t *testing.T) {
	t.Run("existing consumer with shards", func(t *testing.T) {
		c := setup(t)
		grants := c.Grants(prefab.Instance1.ID())
		requireGrantsEqual(t, grants, slicex.New(grant(t, c, "g11"), grant(t, c, "g12"), grant(t, c, "g13")))
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

func TestCluster_Update(t *testing.T) {
	t.Run("assign unknown consumer", func(t *testing.T) {
		c := setup(t)
		old := c.Version()
		g := prefab.NewGrantInfo("g", "t1/s1/d1", model.Regional, "northcentralus", "D", "F", model.ActiveGrant)

		err := c.Update(slicex.New(prefab.Instance1), map[model.ConsumerID][]model.GrantInfo{prefab.Instance4.ID(): slicex.New(g)}, nil, nil, nil)
		require.Error(t, err, "consumer information is missing in assignments: id4")
		require.Equal(t, c.Version(), old)
	})

	t.Run("assign new consumer without grants", func(t *testing.T) {
		c := setup(t)
		old := c.Version()

		err := c.Update(slicex.New(prefab.Instance4), map[model.ConsumerID][]model.GrantInfo{}, nil, nil, nil)
		require.NoError(t, err)
		require.Equal(t, c.Version(), old+1)

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

	t.Run("assign new consumer with grants", func(t *testing.T) {
		c := setup(t)
		old := c.Version()
		g := prefab.NewGrantInfo("g", "t1/s1/d1", model.Regional, "northcentralus", "D", "F", model.ActiveGrant)

		err := c.Update(slicex.New(prefab.Instance4), map[model.ConsumerID][]model.GrantInfo{prefab.Instance4.ID(): slicex.New(g)}, nil, nil, nil)
		require.NoError(t, err)
		require.Equal(t, c.Version(), old+1)

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

	t.Run("assign existing consumer with new grants", func(t *testing.T) {
		c := setup(t)
		old := c.Version()
		g := prefab.NewGrantInfo("g", "t1/s1/d1", model.Regional, "centralus", "B", "C", model.ActiveGrant)
		g21 := grant(t, c, "g21")
		g22 := grant(t, c, "g22")

		err := c.Update(slicex.New(prefab.Instance2), map[model.ConsumerID][]model.GrantInfo{prefab.Instance2.ID(): slicex.New(g)}, nil, nil, nil)
		require.NoError(t, err)
		require.Equal(t, c.Version(), old+1)

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

	t.Run("assign active removed revoked", func(t *testing.T) {
		c := setup(t)
		old := c.Version()
		g13 := prefab.NewGrantInfo("g13", "t1/s1/d1", model.Regional, "eastus1", "E", "F", model.ActiveGrant)

		i, ok := c.OwnerWithState(prefab.NewQDK("t1/s1/d1", "eastus1", "E34345"), model.RevokedGrant)
		require.True(t, ok)
		require.Equal(t, i, prefab.Instance2)

		err := c.Update(slicex.New(prefab.Instance1), map[model.ConsumerID][]model.GrantInfo{prefab.Instance1.ID(): slicex.New(g13)}, nil, nil, nil)
		require.NoError(t, err)
		require.Equal(t, c.Version(), old+1)

		requireOwner(t, c, prefab.NewQDK("t1/s1/d1", "eastus1", "E34345"), prefab.Instance1, model.ActiveGrant)

		_, ok = c.OwnerWithState(prefab.NewQDK("t1/s1/d1", "eastus1", "E34345"), model.RevokedGrant)
		require.False(t, ok)
	})

	t.Run("assign grant moved to new consumer", func(t *testing.T) {
		c := setup(t)
		old := c.Version()
		g12 := prefab.NewGrantInfo("g23", "t1/s1/d1", model.Regional, "northcentralus", "A", "D", model.ActiveGrant)

		requireOwner(t, c, prefab.NewQDK("t1/s1/d1", "northcentralus", "A34345"), prefab.Instance1, model.ActiveGrant)

		err := c.Update(slicex.New(prefab.Instance2), map[model.ConsumerID][]model.GrantInfo{prefab.Instance2.ID(): slicex.New(g12)}, nil, nil, nil)
		require.NoError(t, err)
		require.Equal(t, c.Version(), old+1)

		requireOwner(t, c, prefab.NewQDK("t1/s1/d1", "northcentralus", "A34345"), prefab.Instance2, model.ActiveGrant)
		_, ok := c.Grant("g12")
		require.False(t, ok)
		_, ok = c.Grant("g23")
		require.True(t, ok)
	})

	t.Run("update unknown grant", func(t *testing.T) {
		c := setup(t)
		old := c.Version()

		err := c.Update(nil, nil, slicex.New(prefab.NewGrantInfo("g", "t1/s1/d1", model.Regional, "northcentralus", "0", "A", model.ActiveGrant)), nil, nil)
		require.Errorf(t, err, "unknown grant: g")
		require.Equal(t, c.Version(), old)
	})

	t.Run("activate grant", func(t *testing.T) {
		c := setup(t)
		old := c.Version()

		i, ok := c.OwnerWithState(prefab.NewQDK("t1/s1/d1", "eastus1", "E34345"), model.RevokedGrant)
		require.True(t, ok)
		require.Equal(t, i, prefab.Instance2)

		g13 := prefab.NewGrantInfo("g13", "t1/s1/d1", model.Regional, "eastus1", "E", "F", model.ActiveGrant)
		err := c.Update(nil, nil, slicex.New(g13), nil, nil)
		require.NoError(t, err)
		require.Equal(t, c.Version(), old+1)

		requireOwner(t, c, prefab.NewQDK("t1/s1/d1", "eastus1", "E34345"), prefab.Instance1, model.ActiveGrant)

		_, ok = c.OwnerWithState(prefab.NewQDK("t1/s1/d1", "eastus1", "E34345"), model.RevokedGrant)
	})

	t.Run("unassign unknown grant", func(t *testing.T) {
		c := setup(t)
		old := c.Version()

		err := c.Update(nil, nil, nil, slicex.New[model.GrantID]("g"), nil)
		require.Errorf(t, err, "unknown grant: g")
		require.Equal(t, c.Version(), old)
	})

	t.Run("unassign known grant", func(t *testing.T) {
		c := setup(t)
		old := c.Version()

		_, ok := c.Grant("g13")
		require.True(t, ok)

		err := c.Update(nil, nil, nil, slicex.New[model.GrantID]("g13"), nil)
		require.NoError(t, err)
		require.Equal(t, c.Version(), old+1)

		_, ok = c.Grant("g13")
		require.False(t, ok)
		_, ok = c.GrantConsumer("g13")
		require.False(t, ok)
		requireGrantsEqual(t, c.Grants(prefab.Instance1.ID()), slicex.New(grant(t, c, "g11"), grant(t, c, "g12")))
	})

	t.Run("detach non existing consumer", func(t *testing.T) {
		c := setup(t)
		old := c.Version()

		err := c.Update(nil, nil, nil, nil, slicex.New(prefab.Instance4.ID()))
		require.Errorf(t, err, "unknown consumer: id4")
		require.Equal(t, c.Version(), old)

		// Check that existing consumers are not affected
		requireConsumers(t, c, prefab.Instance1, prefab.Instance2, prefab.Instance3)
		requireOwner(t, c, prefab.NewQDK("t1/s1/d1", "northcentralus", "34345"), prefab.Instance1, model.ActiveGrant)
		requireOwner(t, c, prefab.NewQDK("t1/s1/d1", "centralus", "34345"), prefab.Instance2, model.ActiveGrant)
	})

	t.Run("detach existing consumer", func(t *testing.T) {
		c := setup(t)
		old := c.Version()

		err := c.Update(nil, nil, nil, nil, slicex.New(prefab.Instance1.ID()))
		require.NoError(t, err)
		require.Equal(t, c.Version(), old+1)

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

	t.Run("multiple changes", func(t *testing.T) {
		c := setup(t)
		old := c.Version()

		g11, ok := c.Grant("g11")
		require.True(t, ok)

		g31 := prefab.NewGrantInfo("g31", "t1/s1/d1", model.Regional, "eastus2", "E", "F", model.ActiveGrant)
		g13 := prefab.NewGrantInfo("g13", "t1/s1/d1", model.Regional, "eastus1", "E", "F", model.ActiveGrant)

		assigned := map[model.ConsumerID][]model.GrantInfo{prefab.Instance3.ID(): slicex.New(g31)}
		unassigned := slicex.New[model.GrantID]("g12")
		err := c.Update(slicex.New(prefab.Instance3), assigned, slicex.New(g13), unassigned, slicex.New(prefab.Instance2.ID()))
		require.NoError(t, err)
		require.Equal(t, c.Version(), old+1)

		requireConsumers(t, c, prefab.Instance1, prefab.Instance3)
		requireGrantsEqual(t, c.Grants(prefab.Instance1.ID()), slicex.New(g11, g13))
		requireGrantsEqual(t, c.Grants(prefab.Instance3.ID()), slicex.New(g31))

		_, ok = c.GrantConsumer("g21")
		require.False(t, ok)
	})
}

func TestCluster_Diff(t *testing.T) {

	t.Run("empty old", func(t *testing.T) {
		newCluster := setup(t)
		oldCluster, err := model.NewCluster(model.NewClusterID(), 0, nil, nil)
		require.NoError(t, err)

		d := newCluster.Diff(oldCluster)
		require.Len(t, d.Assigned, 2)
		requireGrantsEqual(t, d.Assigned[prefab.Instance1.ID()], slicex.New(grant(t, newCluster, "g11"), grant(t, newCluster, "g12"), grant(t, newCluster, "g13")))
		requireGrantsEqual(t, d.Assigned[prefab.Instance2.ID()], slicex.New(grant(t, newCluster, "g21"), grant(t, newCluster, "g22")))
		require.Empty(t, d.Updated)
		require.Empty(t, d.Unassigned)
		require.Empty(t, d.Detached)
	})

	t.Run("activation with two grants", func(t *testing.T) {
		g11 := newGrant("g11", "0", "A", model.AllocatedGrant)
		g21 := newGrant("g21", "0", "A", model.RevokedGrant)
		grants := map[model.ConsumerID][]model.GrantInfo{
			prefab.Instance1.ID(): slicex.New(g11),
			prefab.Instance2.ID(): slicex.New(g21),
		}
		oldCluster, err := model.NewCluster(model.NewClusterID(), 0, slicex.New(prefab.Instance1, prefab.Instance2), grants)
		require.NoError(t, err)

		g11 = newGrant("g11", "0", "A", model.ActiveGrant)
		grants = map[model.ConsumerID][]model.GrantInfo{
			prefab.Instance1.ID(): slicex.New(g11),
		}
		newCluster, err := model.NewCluster(model.NewClusterID(), 0, slicex.New(prefab.Instance1, prefab.Instance2), grants)
		require.NoError(t, err)

		d := newCluster.Diff(oldCluster)
		require.Empty(t, d.Assigned)
		require.Len(t, d.Updated, 1)
		requireGrantsEqual(t, d.Updated, slicex.New(g11))
		require.Empty(t, d.Unassigned)
		require.Empty(t, d.Detached)
	})

	t.Run("activation with one grant", func(t *testing.T) {
		g11 := newGrant("g11", "0", "A", model.AllocatedGrant)
		grants := map[model.ConsumerID][]model.GrantInfo{
			prefab.Instance1.ID(): slicex.New(g11),
		}
		oldCluster, err := model.NewCluster(model.NewClusterID(), 0, slicex.New(prefab.Instance1), grants)
		require.NoError(t, err)

		g11 = newGrant("g11", "0", "A", model.ActiveGrant)
		grants = map[model.ConsumerID][]model.GrantInfo{
			prefab.Instance1.ID(): slicex.New(g11),
		}
		newCluster, err := model.NewCluster(model.NewClusterID(), 0, slicex.New(prefab.Instance1), grants)
		require.NoError(t, err)

		d := newCluster.Diff(oldCluster)
		require.Empty(t, d.Assigned)
		require.Len(t, d.Updated, 1)
		requireGrantsEqual(t, d.Updated, slicex.New(g11))
		require.Empty(t, d.Unassigned)
		require.Empty(t, d.Detached)
	})

	t.Run("revoke", func(t *testing.T) {
		g11 := newGrant("g11", "0", "A", model.ActiveGrant)
		grants := map[model.ConsumerID][]model.GrantInfo{
			prefab.Instance1.ID(): slicex.New(g11),
		}
		oldCluster, err := model.NewCluster(model.NewClusterID(), 0, slicex.New(prefab.Instance1, prefab.Instance2), grants)
		require.NoError(t, err)

		g11 = newGrant("g11", "0", "A", model.RevokedGrant)
		g21 := newGrant("g21", "0", "A", model.AllocatedGrant)
		grants = map[model.ConsumerID][]model.GrantInfo{
			prefab.Instance1.ID(): slicex.New(g11),
			prefab.Instance2.ID(): slicex.New(g21),
		}
		newCluster, err := model.NewCluster(model.NewClusterID(), 0, slicex.New(prefab.Instance1, prefab.Instance2), grants)
		require.NoError(t, err)

		d := newCluster.Diff(oldCluster)
		require.Len(t, d.Assigned, 1)
		requireGrantsEqual(t, d.Assigned[prefab.Instance2.ID()], slicex.New(g21))
		require.Empty(t, d.Updated)
		require.Empty(t, d.Unassigned)
		require.Empty(t, d.Detached)
	})

	t.Run("move grant", func(t *testing.T) {
		g11 := newGrant("g11", "0", "A", model.ActiveGrant)
		grants := map[model.ConsumerID][]model.GrantInfo{
			prefab.Instance1.ID(): slicex.New(g11),
		}
		oldCluster, err := model.NewCluster(model.NewClusterID(), 0, slicex.New(prefab.Instance1, prefab.Instance2), grants)
		require.NoError(t, err)

		g21 := newGrant("g21", "0", "A", model.ActiveGrant)
		grants = map[model.ConsumerID][]model.GrantInfo{
			prefab.Instance2.ID(): slicex.New(g21),
		}
		newCluster, err := model.NewCluster(model.NewClusterID(), 0, slicex.New(prefab.Instance1, prefab.Instance2), grants)
		require.NoError(t, err)

		d := newCluster.Diff(oldCluster)
		require.Len(t, d.Assigned, 1)
		requireGrantsEqual(t, d.Assigned[prefab.Instance2.ID()], slicex.New(g21))
		require.Empty(t, d.Updated)
		require.Empty(t, d.Unassigned)
		require.Empty(t, d.Detached)
	})

	for _, state := range slicex.New(model.ActiveGrant, model.RevokedGrant, model.AllocatedGrant) {
		t.Run(fmt.Sprintf("assign %v", state), func(t *testing.T) {
			grants := map[model.ConsumerID][]model.GrantInfo{}
			oldCluster, err := model.NewCluster(model.NewClusterID(), 0, slicex.New(prefab.Instance1, prefab.Instance2), grants)
			require.NoError(t, err)

			g11 := newGrant("g11", "0", "A", state)
			grants = map[model.ConsumerID][]model.GrantInfo{
				prefab.Instance1.ID(): slicex.New(g11),
			}
			newCluster, err := model.NewCluster(model.NewClusterID(), 0, slicex.New(prefab.Instance1, prefab.Instance2), grants)
			require.NoError(t, err)

			d := newCluster.Diff(oldCluster)
			requireGrantsEqual(t, d.Assigned[prefab.Instance1.ID()], slicex.New(g11))
			require.Empty(t, d.Updated)
			require.Empty(t, d.Unassigned)
			require.Empty(t, d.Detached)
		})
	}

	for _, state := range slicex.New(model.ActiveGrant, model.RevokedGrant, model.AllocatedGrant) {
		t.Run(fmt.Sprintf("unassign %v", state), func(t *testing.T) {
			g11 := newGrant("g11", "0", "A", state)
			grants := map[model.ConsumerID][]model.GrantInfo{
				prefab.Instance1.ID(): slicex.New(g11),
			}
			oldCluster, err := model.NewCluster(model.NewClusterID(), 0, slicex.New(prefab.Instance1, prefab.Instance2), grants)
			require.NoError(t, err)

			grants = map[model.ConsumerID][]model.GrantInfo{}
			newCluster, err := model.NewCluster(model.NewClusterID(), 0, slicex.New(prefab.Instance1, prefab.Instance2), grants)
			require.NoError(t, err)

			d := newCluster.Diff(oldCluster)
			require.Empty(t, d.Assigned)
			require.Empty(t, d.Updated)
			requirex.Equal(t, d.Unassigned, slicex.New[model.GrantID]("g11"))
			require.Empty(t, d.Detached)
		})
	}

	t.Run("detach", func(t *testing.T) {
		g11 := newGrant("g11", "0", "A", model.ActiveGrant)
		grants := map[model.ConsumerID][]model.GrantInfo{
			prefab.Instance1.ID(): slicex.New(g11),
		}
		oldCluster, err := model.NewCluster(model.NewClusterID(), 0, slicex.New(prefab.Instance1, prefab.Instance2), grants)
		require.NoError(t, err)

		newCluster, err := model.NewCluster(model.NewClusterID(), 0, slicex.New(prefab.Instance1), grants)
		require.NoError(t, err)

		d := newCluster.Diff(oldCluster)
		require.Empty(t, d.Assigned)
		require.Empty(t, d.Updated)
		require.Empty(t, d.Unassigned)
		requirex.Equal(t, d.Detached, slicex.New(prefab.Instance2.ID()))
	})

}

func setup(t *testing.T) model.Cluster {
	t.Helper()

	g11 := prefab.NewGrantInfo("g11", "t1/s1/d1", model.Regional, "northcentralus", "0", "A", model.ActiveGrant)
	g12 := prefab.NewGrantInfo("g12", "t1/s1/d1", model.Regional, "northcentralus", "A", "D", model.ActiveGrant)
	g13 := prefab.NewGrantInfo("g13", "t1/s1/d1", model.Regional, "eastus1", "E", "F", model.AllocatedGrant)
	g21 := prefab.NewGrantInfo("g21", "t1/s1/d1", model.Regional, "centralus", "0", "A", model.ActiveGrant)
	g22 := prefab.NewGrantInfo("g22", "t1/s1/d1", model.Regional, "eastus1", "E", "F", model.RevokedGrant)
	grants := map[model.ConsumerID][]model.GrantInfo{
		prefab.Instance1.ID(): slicex.New(g11, g12, g13),
		prefab.Instance2.ID(): slicex.New(g21, g22),
		prefab.Instance3.ID(): slicex.New[model.GrantInfo](),
	}
	c, err := model.NewCluster(model.NewClusterID(), 0, slicex.New(prefab.Instance1, prefab.Instance2, prefab.Instance3), grants)
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
		return actual[i].ID() < actual[j].ID()
	})
	expected = slicex.Clone(expected)
	sort.Slice(expected, func(i, j int) bool {
		return expected[i].ID() < expected[j].ID()
	})

	for i := range actual {
		requirex.EqualProtobuf(t, model.UnwrapGrantInfo(actual[i]), model.UnwrapGrantInfo(expected[i]))
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

func grant(t *testing.T, c model.Cluster, gid string) model.GrantInfo {
	t.Helper()
	g, ok := c.Grant(model.GrantID(gid))
	require.True(t, ok)
	return g
}

func newGrant(id string, from, to string, state model.GrantState) model.GrantInfo {
	return prefab.NewGrantInfo(id, "t1/s1/d1", model.Regional, "northcentralus", from, to, state)
}
