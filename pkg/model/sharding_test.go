package model_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"go.atoms.co/lib/testing/assertx"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/testing/prefab"
)

func TestShardMap(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		m := model.NewShardMap[int, int]()

		assert.Len(t, m.Domains(), 0)
		assert.Len(t, m.Lookup(prefab.NewQDK("t/s/d", "", "0")), 0)
	})

	t.Run("unit", func(t *testing.T) {
		m := model.NewShardMap[int, int]()

		unit1 := model.Shard{Domain: prefab.QDN("t/s/d"), Type: model.Unit}
		m.Write(unit1, 1, 1)

		// (1) All keys for that domain match the unit

		assert.Len(t, m.Lookup(prefab.NewQDK("t/s/d", "", "5")), 1)
		assert.Len(t, m.Lookup(prefab.NewQDK("t/s/d", "us", "9")), 1)

		m.Write(unit1, 2, 2)

		assert.Len(t, m.Lookup(prefab.NewQDK("t/s/d", "", "5")), 2)
		assert.Len(t, m.Lookup(prefab.NewQDK("t/s/d", "us", "9")), 2)

		m.Delete(unit1, 1)

		assert.Len(t, m.Lookup(prefab.NewQDK("t/s/d", "", "5")), 1)
		assert.Len(t, m.Lookup(prefab.NewQDK("t/s/d", "us", "9")), 1)
	})

	t.Run("global", func(t *testing.T) {
		global1 := prefab.NewShard("t/s/d", model.Global, "", "0", "1")
		global2 := prefab.NewShard("t/s/d", model.Global, "", "1", "2")
		global3 := prefab.NewShard("t/s/d", model.Global, "", "2", "4")

		m := model.NewShardMap[int, int]()

		// (1) Two disjoint shards

		m.Write(global1, 1, 1)
		m.Write(global2, 2, 2)

		assert.Len(t, m.Domains(), 1)

		list := m.Lookup(prefab.NewQDK("t/s/d", "", "0"))
		assert.Len(t, list, 1)
		assertx.Equal(t, list[0].Shard, global1)
		list = m.Lookup(prefab.NewQDK("t/s/d", "", "1"))
		assert.Len(t, list, 1)
		assertx.Equal(t, list[0].Shard, global2)

		assert.Len(t, m.Lookup(prefab.NewQDK("t/s/d", "", "0")), 1)
		assert.Len(t, m.Lookup(prefab.NewQDK("t/s/d", "", "1")), 1)
		assert.Len(t, m.Lookup(prefab.NewQDK("t/s/d", "", "2")), 0)
		assert.Len(t, m.Lookup(prefab.NewQDK("t/s/d", "", "3")), 0)

		assert.Len(t, m.Lookup(prefab.NewQDK("t/s/d2", "", "1")), 0)

		// (2) Add shards

		m.Write(global2, 3, 3) // "duplicate" (w/ different key)
		m.Write(global3, 4, 4)
		m.Write(global2, 5, 5) // "duplicate" (w/ different key)

		assert.Len(t, m.Lookup(prefab.NewQDK("t/s/d", "", "0")), 1)
		assert.Len(t, m.Lookup(prefab.NewQDK("t/s/d", "", "1")), 3)
		assert.Len(t, m.Lookup(prefab.NewQDK("t/s/d", "", "2")), 1)
		assert.Len(t, m.Lookup(prefab.NewQDK("t/s/d", "", "3")), 1)

		// (3) Remove shards

		m.Delete(global1, 2) // no such key

		assert.Len(t, m.Lookup(prefab.NewQDK("t/s/d", "", "0")), 1)

		m.Delete(global1, 1)
		m.Delete(global2, 3)

		assert.Len(t, m.Lookup(prefab.NewQDK("t/s/d", "", "0")), 0)
		assert.Len(t, m.Lookup(prefab.NewQDK("t/s/d", "", "1")), 2)
		assert.Len(t, m.Lookup(prefab.NewQDK("t/s/d", "", "2")), 1)
		assert.Len(t, m.Lookup(prefab.NewQDK("t/s/d", "", "3")), 1)
	})

	t.Run("regional", func(t *testing.T) {
		us1 := prefab.NewShard("t/s/d", model.Regional, "us", "0", "1")
		us2 := prefab.NewShard("t/s/d", model.Regional, "us", "1", "2")
		eu1 := prefab.NewShard("t/s/d", model.Regional, "eu", "0", "1")

		m := model.NewShardMap[int, int]()

		// (1) Regions are disjoint, like different domains

		m.Write(us1, 1, 1)
		m.Write(us2, 1, 1)
		m.Write(eu1, 1, 1)

		assert.Len(t, m.Domains(), 1)
		assert.Len(t, m.Domain(us1.Domain), 3)

		assert.Len(t, m.Lookup(prefab.NewQDK("t/s/d", "us", "0")), 1)
		assert.Len(t, m.Lookup(prefab.NewQDK("t/s/d", "us", "1")), 1)
		assert.Len(t, m.Lookup(prefab.NewQDK("t/s/d", "us", "2")), 0)

		assert.Len(t, m.Lookup(prefab.NewQDK("t/s/d", "eu", "0")), 1)
		assert.Len(t, m.Lookup(prefab.NewQDK("t/s/d", "eu", "1")), 0)
		assert.Len(t, m.Lookup(prefab.NewQDK("t/s/d", "eu", "2")), 0)
	})

	t.Run("sparse", func(t *testing.T) {
		global1 := prefab.NewShard("t/s/d", model.Global, "", "0", "1")
		global2 := prefab.NewShard("t/s/d", model.Global, "", "2", "3")
		global3 := prefab.NewShard("t/s/d", model.Global, "", "3", "4")
		global4 := prefab.NewShard("t/s/d", model.Global, "", "7", "9")

		m := model.NewShardMap[int, int]()

		// (1) Two disjoint shards

		m.Write(global1, 1, 1)
		m.Write(global2, 1, 1)
		m.Write(global3, 1, 1)
		m.Write(global4, 1, 1)

		m.Write(global3, 2, 2)

		assert.Len(t, m.Domains(), 1)

		assert.Len(t, m.Lookup(prefab.NewQDK("t/s/d", "", "0")), 1)
		assert.Len(t, m.Lookup(prefab.NewQDK("t/s/d", "", "1")), 0)
		assert.Len(t, m.Lookup(prefab.NewQDK("t/s/d", "", "2")), 1)
		assert.Len(t, m.Lookup(prefab.NewQDK("t/s/d", "", "3")), 2)
		assert.Len(t, m.Lookup(prefab.NewQDK("t/s/d", "", "4")), 0)
		assert.Len(t, m.Lookup(prefab.NewQDK("t/s/d", "", "5")), 0)
		assert.Len(t, m.Lookup(prefab.NewQDK("t/s/d", "", "6")), 0)
		assert.Len(t, m.Lookup(prefab.NewQDK("t/s/d", "", "7")), 1)
	})
}
