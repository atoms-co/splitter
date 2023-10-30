package allocation_test

import (
	"go.atoms.co/splitter/lib/service/location"
	"go.atoms.co/lib/testing/assertx"
	"go.atoms.co/lib/testing/mockclock"
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

		grants, ok := alloc.Attach(allocation.Worker{ID: "foo", Lease: cl.Now().Add(time.Minute)})
		assert.True(t, ok)
		assert.Len(t, grants, 0)

		grants = alloc.Allocate(cl.Now())
		assert.Len(t, grants, 0) // no work
	})

	t.Run("attach", func(t *testing.T) {
		// Basic allocation to single attaching/detaching worker

		alloc := allocation.New[string, int]("id", nil, nil, work, cl.Now())
		assert.Len(t, alloc.Work(), 3)

		// (1) Attach with external existing grant

		foo := allocation.Worker{ID: "foo", Lease: cl.Now().Add(time.Minute)}

		old := allocation.Grant[string, int]{
			ID:         "old:42",
			State:      allocation.Active,
			Unit:       "c",
			Domain:     0,
			Worker:     foo.ID,
			Assigned:   cl.Now().Add(-time.Hour),
			Expiration: cl.Now().Add(-time.Second), // expiration time is irrelevant
		}

		initial, ok := alloc.Attach(foo, old)
		assert.True(t, ok)
		require.Len(t, initial, 1)
		assertx.Equal(t, initial[0].ID, old.ID) // keep old ID and information ..
		assertx.Equal(t, initial[0].State, allocation.Active)
		assertx.Equal(t, initial[0].Unit, old.Unit)
		assertx.Equal(t, initial[0].Assigned, old.Assigned)
		assertx.Equal(t, initial[0].Expiration, foo.Lease) // .. but updated expiration

		_, ok = alloc.Attach(foo)
		assert.False(t, ok) // can't attach if already attached

		// (2) Allocate with attached worker

		grants := alloc.Allocate(cl.Now())
		assert.Len(t, grants, 2)

		for _, g := range grants {
			assertx.Equal(t, g.State, allocation.Active)
			assertx.Equal(t, g.Worker, foo.ID)
			assertx.Equal(t, g.Assigned, cl.Now())
			assertx.Equal(t, g.Expiration, foo.Lease)
		}

		m := allocation.NewGrantMap(grants...)
		assert.Len(t, m, 2)

		grants, ok = alloc.Assigned(foo.ID)
		assert.True(t, ok)
		assert.Len(t, grants, 3)

		// (3) Detach and see grants inactive

		inactive, ok := alloc.Detach(foo.ID)
		assert.True(t, ok)
		assert.Len(t, inactive, 3)

		for _, g := range inactive {
			assertx.Equal(t, g.State, allocation.Inactive)
			assertx.Equal(t, g.Worker, foo.ID)
			assertx.Equal(t, g.Expiration, foo.Lease)
		}

		_, ok = alloc.Detach(foo.ID)
		assert.False(t, ok) // can't detach if not attached
		_, ok = alloc.Assigned(foo.ID)
		assert.False(t, ok) // nothing is assigned to a detached worker

		// (4) Re-attach with various grant conditions 10s later

		foo2 := allocation.Worker{ID: "foo", Lease: cl.Now().Add(2 * time.Minute)}
		cl.Add(10 * time.Second)

		// (a) old grant

		regrants, ok := alloc.Attach(foo2, m["a"])
		assert.True(t, ok)
		require.Len(t, regrants, 1)
		assertx.Equal(t, regrants[0].State, allocation.Active)
		assertx.Equal(t, regrants[0].Unit, "a")
		assertx.Equal(t, regrants[0].Assigned, m["a"].Assigned) // original assignment time ..
		assertx.Equal(t, regrants[0].Expiration, foo2.Lease)    // .. but updated expiration

		_, ok = alloc.Detach(foo2.ID)
		assert.True(t, ok)

		// (b) conflicting grant id

		bad1 := m["b"]
		bad1.ID = "bad"

		regrants, ok = alloc.Attach(foo2, bad1)
		assert.True(t, ok)
		require.Len(t, regrants, 0)

		_, ok = alloc.Detach(foo2.ID)
		assert.True(t, ok)

		// (c) conflicting worker id

		bad2 := m["b"]
		bad2.Worker = "bar"

		regrants, ok = alloc.Attach(foo2, bad2)
		assert.True(t, ok)
		require.Len(t, regrants, 0)

		_, ok = alloc.Detach(foo2.ID)
		assert.True(t, ok)

		// (d) unrecognized unit

		bad3 := m["b"]
		bad3.Unit = "bad"

		regrants, ok = alloc.Attach(foo2, bad3)
		assert.True(t, ok)
		require.Len(t, regrants, 0)

		grants, ok = alloc.Assigned(foo.ID)
		assert.True(t, ok)
		assert.Len(t, grants, 0)
	})

	t.Run("allocate/constraints", func(t *testing.T) {
		// Single allocation with region affinity constraint in various worker situations

		region := allocation.NewConstraint(allocation.RegionAffinityRule, allocation.HasRegionAffinity[string, int])

		alloc := allocation.New[string, int]("id", []allocation.Placement[string, int]{region}, nil, work, cl.Now())
		assert.Len(t, alloc.Work(), 3)

		// (1) Attach us worker only. Allocate grants only us (a+b) work.

		us1 := allocation.Worker{ID: "us1", Location: us, Lease: cl.Now().Add(time.Minute)}
		eu1 := allocation.Worker{ID: "eu1", Location: eu, Lease: cl.Now().Add(time.Minute)}

		_, ok := alloc.Attach(us1)
		assert.True(t, ok)

		grants := alloc.Allocate(cl.Now())
		assert.Len(t, grants, 2)

		m := allocation.NewGrantMap(grants...)
		assertx.Equal(t, m["a"].Worker, us1.ID)
		assertx.Equal(t, m["b"].Worker, us1.ID)

		// (2) Attach eu worker to allocate eu work.

		_, ok = alloc.Attach(eu1)
		assert.True(t, ok)

		grants = alloc.Allocate(cl.Now())
		assert.Len(t, grants, 1)

		m = allocation.NewGrantMap(grants...)
		assertx.Equal(t, m["c"].Worker, eu1.ID)

		// (2) If eu1 detaches, changes region, and re-attaches it fails to revive its _current_ grant.

		_, ok = alloc.Detach(eu1.ID)
		assert.True(t, ok)

		eu1.Location = us

		regrants, ok := alloc.Attach(eu1, m["c"])
		assert.True(t, ok)
		assert.Len(t, regrants, 0)
	})

	t.Run("allocate/region-affinity", func(t *testing.T) {
		// Single allocation with region affinity preference

		region := allocation.NewPreference(allocation.RegionAffinityRule, 5, allocation.HasRegionAffinity[string, int])

		alloc := allocation.New[string, int]("id", []allocation.Placement[string, int]{region}, nil, work, cl.Now())
		assert.Len(t, alloc.Work(), 3)

		// (1) Allocate picks lowest penalties, even if small. So jp1 receives no work.

		us1 := allocation.Worker{ID: "us1", Location: us, Lease: cl.Now()}
		eu1 := allocation.Worker{ID: "eu1", Location: eu, Lease: cl.Now().Add(time.Minute)}
		jp1 := allocation.Worker{ID: "jp1", Location: jp, Lease: cl.Now().Add(time.Minute)}

		_, ok := alloc.Attach(us1)
		assert.True(t, ok)
		_, ok = alloc.Attach(eu1)
		assert.True(t, ok)
		_, ok = alloc.Attach(jp1)
		assert.True(t, ok)

		grants := alloc.Allocate(cl.Now())
		assert.Len(t, grants, 3)

		m := allocation.NewGrantMap(grants...)
		assertx.Equal(t, m["a"].Worker, us1.ID)
		assertx.Equal(t, m["b"].Worker, us1.ID)
		assertx.Equal(t, m["c"].Worker, eu1.ID)

		// (2) If us1 detaches and grants expire, jp1 will receive "a" (20 + 5 load) and eu1 will receive
		// "b" (10 + 5 load) to complement "c" (10 load). That represents the best placement.

		_, ok = alloc.Detach(us1.ID)
		assert.True(t, ok)

		cl.Add(time.Second)

		promo := alloc.Expire(cl.Now())
		assert.Len(t, promo, 0) // no expired Revoked grants

		grants = alloc.Allocate(cl.Now())
		assert.Len(t, grants, 2)

		m = allocation.NewGrantMap(grants...)
		assertx.Equal(t, m["a"].Worker, jp1.ID)
		assertx.Equal(t, m["b"].Worker, eu1.ID)
	})
}
