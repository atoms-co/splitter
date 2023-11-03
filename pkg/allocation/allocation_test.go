package allocation_test

import (
	"go.atoms.co/splitter/lib/service/location"
	"go.atoms.co/lib/testing/assertx"
	"go.atoms.co/lib/testing/mockclock"
	"go.atoms.co/lib/mapx"
	"go.atoms.co/slicex"
	"go.atoms.co/splitter/pkg/allocation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

var (
	us = location.Location{Region: "us"}
	eu = location.Location{Region: "eu"}
	jp = location.Location{Region: "jp"}
)

func TestAllocation(t *testing.T) {
	cl := mockclock.NewUnsynchronized()
	cl.Set(time.Now())

	work := []allocation.Work[string, int]{
		{Unit: "a", Load: 20, Location: us},
		{Unit: "b", Load: 10, Location: us},
		{Unit: "c", Load: 10, Location: eu},
	}

	t.Run("empty", func(t *testing.T) {
		// Empty allocation should be a nop, but valid.

		alloc := allocation.New[string, int]("id", nil, nil, nil, cl.Now())
		assert.Len(t, alloc.Work(), 0)
		assert.Len(t, alloc.Workers(), 0)

		grants := alloc.Allocate(cl.Now())
		assert.Len(t, grants, 0) // no workers/no work

		assignments, ok := alloc.Attach(allocation.Worker{ID: "foo"}, cl.Now().Add(time.Minute))
		assert.True(t, ok)
		assert.Len(t, assignments.Active, 0)
		assert.Len(t, assignments.Allocated, 0)
		assert.Len(t, assignments.Revoked, 0)

		grants = alloc.Allocate(cl.Now())
		assert.Len(t, grants, 0) // no work
	})

	t.Run("attach", func(t *testing.T) {
		// Basic allocation to single attaching/detaching worker

		alloc := allocation.New[string, int]("id", nil, nil, work, cl.Now())
		assert.Len(t, alloc.Work(), 3)

		// (1) Attach with external existing grant. Bad grants are ignored

		foo := allocation.Worker{ID: "foo"}
		lease := cl.Now().Add(time.Minute)

		old := allocation.Grant[string, int]{
			ID:         "old:42",
			State:      allocation.Active,
			Unit:       "c",
			Domain:     0,
			Worker:     foo.ID,
			Assigned:   cl.Now().Add(-time.Hour),
			Expiration: cl.Now().Add(-time.Second), // expiration time is irrelevant
		}
		bad := allocation.Grant[string, int]{
			ID:         "old:1",
			State:      allocation.Active,
			Unit:       "bad",
			Domain:     0,
			Worker:     foo.ID,
			Assigned:   cl.Now().Add(-time.Hour),
			Expiration: cl.Now().Add(-time.Second),
		}

		initial, ok := alloc.Attach(foo, lease, old, bad)
		assert.True(t, ok)
		require.Len(t, initial.Active, 1)
		assertx.Equal(t, initial.Active[0].ID, old.ID) // keep old ID and information ..
		assertx.Equal(t, initial.Active[0].State, allocation.Active)
		assertx.Equal(t, initial.Active[0].Unit, old.Unit)
		assertx.Equal(t, initial.Active[0].Assigned, old.Assigned)
		assertx.Equal(t, initial.Active[0].Expiration, lease) // .. but updated expiration

		_, ok = alloc.Attach(foo, lease)
		assert.False(t, ok) // can't attach if already attached

		// (2) Allocate with attached worker

		grants := alloc.Allocate(cl.Now())
		assert.Len(t, grants, 2)

		for _, g := range grants {
			assertx.Equal(t, g.State, allocation.Active)
			assertx.Equal(t, g.Worker, foo.ID)
			assertx.Equal(t, g.Assigned, cl.Now())
			assertx.Equal(t, g.Expiration, lease)
		}

		m := newGrantMap(grants...)
		assert.Len(t, m, 2)

		assignments := alloc.Assigned(foo.ID)
		assert.Len(t, assignments.Active, 3)

		// (3) Detach

		ok = alloc.Detach(foo.ID)
		assert.True(t, ok)

		ok = alloc.Detach(foo.ID)
		assert.False(t, ok) // can't detach if not attached

		assignments = alloc.Assigned(foo.ID)
		assert.Len(t, assignments.Active, 3) // still assigned

		// (4) Re-attach 10s later restores old state, regardless of grants passed

		lease2min := cl.Now().Add(2 * time.Minute)
		cl.Add(10 * time.Second)

		regrants, ok := alloc.Attach(foo, lease2min, m["a"], bad)
		assert.True(t, ok)
		require.Len(t, regrants.Active, 3)
		require.Len(t, regrants.Revoked, 0)
	})

	t.Run("allocate/constraints", func(t *testing.T) {
		// Single allocation with region affinity constraint in various worker situations

		region := allocation.NewConstraint(allocation.RegionAffinityRule, allocation.HasRegionAffinity[string, int])

		alloc := allocation.New[string, int]("id", slicex.New(region), nil, work, cl.Now())
		assert.Len(t, alloc.Work(), 3)

		// (1) Attach us worker only. Allocate grants only us (a+b) work.

		lease := cl.Now().Add(time.Minute)

		us1 := allocation.Worker{ID: "us1", Location: us}
		eu1 := allocation.Worker{ID: "eu1", Location: eu}

		_, ok := alloc.Attach(us1, lease)
		assert.True(t, ok)

		grants := alloc.Allocate(cl.Now())
		assert.Len(t, grants, 2)

		m := newGrantMap(grants...)
		assertx.Equal(t, m["a"].Worker, us1.ID)
		assertx.Equal(t, m["b"].Worker, us1.ID)

		// (2) Attach eu worker to allocate eu work.

		_, ok = alloc.Attach(eu1, lease)
		assert.True(t, ok)

		grants = alloc.Allocate(cl.Now())
		assert.Len(t, grants, 1)

		m = newGrantMap(grants...)
		assertx.Equal(t, m["c"].Worker, eu1.ID)
	})

	t.Run("allocate/region-affinity", func(t *testing.T) {
		// Single allocation with region affinity preference

		region := allocation.NewPreference(allocation.RegionAffinityRule, 5, allocation.HasRegionAffinity[string, int])

		alloc := allocation.New[string, int]("id", slicex.New(region), nil, work, cl.Now())
		assert.Len(t, alloc.Work(), 3)

		// (1) Allocate picks lowest penalties, even if small. So jp1 receives no work.

		lease := cl.Now().Add(time.Minute)

		us1 := allocation.Worker{ID: "us1", Location: us}
		eu1 := allocation.Worker{ID: "eu1", Location: eu}
		jp1 := allocation.Worker{ID: "jp1", Location: jp}

		_, ok := alloc.Attach(us1, cl.Now())
		assert.True(t, ok)
		_, ok = alloc.Attach(eu1, lease)
		assert.True(t, ok)
		_, ok = alloc.Attach(jp1, lease)
		assert.True(t, ok)

		grants := alloc.Allocate(cl.Now())
		assert.Len(t, grants, 3)

		m := newGrantMap(grants...)
		assertx.Equal(t, m["a"].Worker, us1.ID)
		assertx.Equal(t, m["b"].Worker, us1.ID)
		assertx.Equal(t, m["c"].Worker, eu1.ID)

		// (2) If us1 detaches and grants expire, jp1 will receive "a" (20 + 5 load) and eu1 will receive
		// "b" (10 + 5 load) to complement "c" (10 load). That represents the best placement.

		ok = alloc.Detach(us1.ID)
		assert.True(t, ok)

		cl.Add(time.Second)

		promo := alloc.Expire(cl.Now())
		assert.Len(t, promo, 0)

		grants = alloc.Allocate(cl.Now())
		require.Len(t, grants, 2)

		m = newGrantMap(grants...)
		assertx.Equal(t, m["a"].Worker, jp1.ID)
		assertx.Equal(t, m["b"].Worker, eu1.ID)
	})

	t.Run("revoke", func(t *testing.T) {
		// Revoke/release functionality

		alloc := allocation.New[string, int]("id", nil, nil, work, cl.Now())
		assert.Len(t, alloc.Work(), 3)

		// (1) Attach 1 worker and allocate.

		lease := cl.Now().Add(time.Minute)

		us1 := allocation.Worker{ID: "us1", Location: us}
		us2 := allocation.Worker{ID: "us2", Location: us}

		_, ok := alloc.Attach(us1, lease)
		assert.True(t, ok)
		grants := alloc.Allocate(cl.Now())
		assert.Len(t, grants, 3)

		// (2) Revoke is not possible, because it cannot move anywhere.

		moves, rejects := alloc.Revoke(us1.ID, cl.Now(), grants[0])
		assert.Len(t, moves, 0)
		assert.Len(t, rejects, 1)

		// (2) Attach another worker and it can

		_, ok = alloc.Attach(us2, lease)
		assert.True(t, ok)

		moves, rejects = alloc.Revoke(us1.ID, cl.Now(), grants[0])
		require.Len(t, moves, 1)
		assert.Len(t, rejects, 0)

		assertx.Equal(t, moves[0].From.Worker, us1.ID)
		assertx.Equal(t, moves[0].From.Unit, grants[0].Unit)
		assertx.Equal(t, moves[0].From.State, allocation.Revoked)
		assertx.Equal(t, moves[0].To.Worker, us2.ID)
		assertx.Equal(t, moves[0].To.Unit, grants[0].Unit)
		assertx.Equal(t, moves[0].To.State, allocation.Allocated)

		// (3) Release and the new grant is promoted Active

		promo, ok := alloc.Release(moves[0].From)
		assert.True(t, ok)
		assertx.Equal(t, promo.ID, moves[0].To.ID)
		assertx.Equal(t, promo.Worker, us2.ID)
		assertx.Equal(t, promo.Unit, grants[0].Unit)
		assertx.Equal(t, promo.State, allocation.Active)
	})

	t.Run("update", func(t *testing.T) {
		// Update functionality

		alloc := allocation.New[string, int]("id", nil, nil, work, cl.Now())
		assert.Len(t, alloc.Work(), 3)

		// (1) Setup a situation: a, revoked + b, c active.

		lease := cl.Now().Add(time.Minute)

		us1 := allocation.Worker{ID: "us1", Location: us}
		us2 := allocation.Worker{ID: "us2", Location: us}

		_, ok := alloc.Attach(us1, lease)
		assert.True(t, ok)
		_, ok = alloc.Attach(us2, lease)
		assert.True(t, ok)

		grants := alloc.Allocate(cl.Now())
		assert.Len(t, grants, 3)

		m := newGrantMap(grants...)
		alloc.Revoke(m["a"].Worker, cl.Now(), m["a"])

		assignments := alloc.Assigned(m["a"].Worker)
		require.Len(t, assignments.Active, 0)
		require.Len(t, assignments.Revoked, 1)

		assignments = alloc.Assigned(m["b"].Worker)
		require.Len(t, assignments.Active, 2)
		require.Len(t, assignments.Allocated, 1)
		require.Len(t, assignments.Revoked, 0)

		// (2) Update work to remove b and add d. Revoke status of a is preserved.

		upd := []allocation.Work[string, int]{
			{Unit: "a", Load: 20, Location: us},
			{Unit: "c", Load: 10, Location: eu},
			{Unit: "d", Load: 10, Location: eu},
		}

		alloc2, rejects := allocation.Update(alloc, upd, cl.Now())
		require.Len(t, rejects, 1)
		assertx.Equal(t, rejects[0].Unit, "b")

		assignments = alloc2.Assigned(m["a"].Worker)
		require.Len(t, assignments.Active, 0)
		require.Len(t, assignments.Revoked, 1)

		assignments = alloc2.Assigned(m["b"].Worker)
		require.Len(t, assignments.Active, 1)
		require.Len(t, assignments.Allocated, 1)
		require.Len(t, assignments.Revoked, 0)

		grants = alloc2.Allocate(cl.Now())
		require.Len(t, grants, 1)
		assertx.Equal(t, grants[0].Unit, "d")
	})
}

func newGrantMap[T, D comparable](list ...allocation.Grant[T, D]) map[T]allocation.Grant[T, D] {
	return mapx.New(list, func(v allocation.Grant[T, D]) T {
		return v.Unit
	})
}
