package allocation_test

import (
	"slices"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.atoms.co/splitter/lib/service/location"
	"go.atoms.co/lib/testing/assertx"
	"go.atoms.co/lib/testing/mockclock"
	"go.atoms.co/lib/testing/requirex"
	"go.atoms.co/lib/mapx"
	"go.atoms.co/slicex"
	"go.atoms.co/splitter/pkg/allocation"
)

var (
	us = location.Location{Region: "us"}
	eu = location.Location{Region: "eu"}
	jp = location.Location{Region: "jp"}
)

func hasRegionAffinity(worker allocation.Worker[string, location.Location], work allocation.Work[string, location.Location]) bool {
	return work.Data.Region == "" || worker.Data.Region == work.Data.Region
}

type regionBan struct {
	region location.Region
}

func newRegionBan(region location.Region) allocation.Placement[string, location.Location, string, location.Location] {
	return &regionBan{
		region: region,
	}
}

func (r regionBan) ID() allocation.Rule {
	return "region-ban"
}

func (r regionBan) TryPlace(worker allocation.Worker[string, location.Location], work allocation.Work[string, location.Location]) (allocation.Load, bool) {
	if worker.Data.Region == r.region {
		return 0, false
	}
	return 0, true
}

func TestAllocation(t *testing.T) {
	cl := mockclock.NewUnsynchronized()
	cl.Set(time.Now())

	work := []allocation.Work[string, location.Location]{
		{Unit: "a", Load: 20, Data: us},
		{Unit: "b", Load: 10, Data: us},
		{Unit: "c", Load: 10, Data: eu},
	}

	t.Run("empty", func(t *testing.T) {
		// Empty allocation should be a nop, but valid.

		alloc := allocation.New[string, location.Location, string, location.Location]("id", nil, nil, nil, cl.Now())
		assert.Len(t, alloc.Work(), 0)
		assert.Len(t, alloc.Workers(), 0)
		require.NoError(t, alloc.Check())

		grants := alloc.Allocate(cl.Now())
		assert.Len(t, grants, 0) // no workers/no work
		require.NoError(t, alloc.Check())

		assignments, ok := alloc.Attach(allocation.Worker[string, location.Location]{ID: "foo", Data: us}, allocation.NoCapacityLimit, cl.Now().Add(time.Minute))
		assert.True(t, ok)
		assert.Len(t, assignments.Active, 0)
		assert.Len(t, assignments.Allocated, 0)
		assert.Len(t, assignments.Revoked, 0)
		require.NoError(t, alloc.Check())

		grants = alloc.Allocate(cl.Now())
		assert.Len(t, grants, 0) // no work
	})

	t.Run("attach", func(t *testing.T) {
		// Basic allocation to single attaching/detaching worker

		alloc := allocation.New[string, location.Location, string, location.Location]("id", nil, nil, work, cl.Now())
		assert.Len(t, alloc.Work(), 3)

		// (1) Attach with external existing grant. Bad grants are ignored

		foo := allocation.Worker[string, location.Location]{ID: "foo", Data: us}
		lease := cl.Now().Add(time.Minute)

		old := allocation.Grant[string, string]{
			ID:         "old:42",
			State:      allocation.Active,
			Unit:       "c",
			Worker:     foo.ID,
			Assigned:   cl.Now().Add(-time.Hour),
			Expiration: cl.Now().Add(-time.Second), // expiration time is irrelevant
		}
		bad := allocation.Grant[string, string]{
			ID:         "old:1",
			State:      allocation.Active,
			Unit:       "bad",
			Worker:     foo.ID,
			Assigned:   cl.Now().Add(-time.Hour),
			Expiration: cl.Now().Add(-time.Second),
		}

		initial, ok := alloc.Attach(foo, allocation.NoCapacityLimit, lease, old, bad)
		assert.True(t, ok)
		require.Len(t, initial.Active, 1)
		assertx.Equal(t, initial.Active[0].ID, old.ID) // keep old ID and information ..
		assertx.Equal(t, initial.Active[0].State, allocation.Active)
		assertx.Equal(t, initial.Active[0].Unit, old.Unit)
		assertx.Equal(t, initial.Active[0].Assigned, old.Assigned)
		assertx.Equal(t, initial.Active[0].Expiration, lease) // .. but updated expiration
		require.NoError(t, alloc.Check())

		_, ok = alloc.Attach(foo, allocation.NoCapacityLimit, lease)
		assert.False(t, ok) // can't attach if already attached
		require.NoError(t, alloc.Check())

		// (2) Allocate with attached worker

		grants := alloc.Allocate(cl.Now())
		assert.Len(t, grants, 2)
		require.NoError(t, alloc.Check())

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
		require.NoError(t, alloc.Check())

		ok = alloc.Detach(foo.ID)
		assert.False(t, ok) // can't detach if not attached
		require.NoError(t, alloc.Check())

		assignments = alloc.Assigned(foo.ID)
		assert.Len(t, assignments.Active, 3) // still assigned

		// (4) Re-attach 10s later restores old state, regardless of grants passed

		lease2min := cl.Now().Add(2 * time.Minute)
		cl.Add(10 * time.Second)

		regrants, ok := alloc.Attach(foo, allocation.NoCapacityLimit, lease2min, m["a"], bad)
		assert.True(t, ok)
		require.Len(t, regrants.Active, 3)
		require.Len(t, regrants.Revoked, 0)
		require.NoError(t, alloc.Check())
	})

	t.Run("suspend", func(t *testing.T) {
		// Suspend does not allow allocation

		alloc := allocation.New[string, location.Location, string, location.Location]("id", nil, nil, work, cl.Now())
		assert.Len(t, alloc.Work(), 3)

		// (1) Attach 1 worker, suspend it and allocate. No grants are created

		us1 := allocation.Worker[string, location.Location]{ID: "us1", Data: us}
		_, ok := alloc.Attach(us1, allocation.NoCapacityLimit, cl.Now().Add(time.Minute))
		assert.True(t, ok)
		require.NoError(t, alloc.Check())

		info, ok := alloc.Suspend(us1.ID)
		assert.True(t, ok)
		assertx.Equal(t, info.State, allocation.Suspended)
		require.NoError(t, alloc.Check())

		grants := alloc.Allocate(cl.Now())
		assert.Len(t, grants, 0)
		require.NoError(t, alloc.Check())
	})

	t.Run("allocate/capacity", func(t *testing.T) {
		// Single allocation with workers with various capacity limits

		alloc := allocation.New[string, location.Location, string, location.Location]("id", nil, nil, work, cl.Now())
		assert.Len(t, alloc.Work(), 3)

		lease := cl.Now().Add(time.Minute)

		w1 := allocation.Worker[string, location.Location]{ID: "w1", Data: us}
		w2 := allocation.Worker[string, location.Location]{ID: "w2", Data: us}
		w3 := allocation.Worker[string, location.Location]{ID: "w3", Data: us}
		w4 := allocation.Worker[string, location.Location]{ID: "w4", Data: us}

		_, ok := alloc.Attach(w1, 10, lease)
		assert.True(t, ok)
		grants := alloc.Allocate(cl.Now())
		assert.Len(t, grants, 1)

		_, ok = alloc.Attach(w2, 10, lease)
		assert.True(t, ok)
		grants = alloc.Allocate(cl.Now())
		assert.Len(t, grants, 1)

		_, ok = alloc.Attach(w3, 10, lease)
		assert.True(t, ok)
		grants = alloc.Allocate(cl.Now())
		assert.Len(t, grants, 0)

		_, ok = alloc.Attach(w4, allocation.NoCapacityLimit, lease)
		assert.True(t, ok)
		grants = alloc.Allocate(cl.Now())
		assert.Len(t, grants, 1)
	})

	t.Run("allocate/constraints", func(t *testing.T) {
		// Single allocation with region affinity constraint in various worker situations

		region := allocation.NewConstraint("region-affinity", hasRegionAffinity)

		alloc := allocation.New[string, location.Location, string, location.Location]("id", slicex.New(region), nil, work, cl.Now())
		assert.Len(t, alloc.Work(), 3)

		// (1) Attach us worker only. Allocate grants only us (a+b) work.

		lease := cl.Now().Add(time.Minute)

		us1 := allocation.Worker[string, location.Location]{ID: "us1", Data: us}
		eu1 := allocation.Worker[string, location.Location]{ID: "eu1", Data: eu}

		_, ok := alloc.Attach(us1, allocation.NoCapacityLimit, lease)
		assert.True(t, ok)

		grants := alloc.Allocate(cl.Now())
		assert.Len(t, grants, 2)

		m := newGrantMap(grants...)
		assertx.Equal(t, m["a"].Worker, us1.ID)
		assertx.Equal(t, m["b"].Worker, us1.ID)

		// (2) Attach eu worker to allocate eu work.

		_, ok = alloc.Attach(eu1, allocation.NoCapacityLimit, lease)
		assert.True(t, ok)

		grants = alloc.Allocate(cl.Now())
		assert.Len(t, grants, 1)

		m = newGrantMap(grants...)
		assertx.Equal(t, m["c"].Worker, eu1.ID)
	})

	t.Run("allocate/region-affinity", func(t *testing.T) {
		// Single allocation with region affinity preference

		region := allocation.NewPreference("region-affinity", 5, hasRegionAffinity)

		alloc := allocation.New[string, location.Location, string, location.Location]("id", slicex.New(region), nil, work, cl.Now())
		assert.Len(t, alloc.Work(), 3)

		// (1) Allocate picks lowest penalties, even if small. So jp1 receives no work.

		lease := cl.Now().Add(time.Minute)

		us1 := allocation.Worker[string, location.Location]{ID: "us1", Data: us}
		eu1 := allocation.Worker[string, location.Location]{ID: "eu1", Data: eu}
		jp1 := allocation.Worker[string, location.Location]{ID: "jp1", Data: jp}

		_, ok := alloc.Attach(us1, allocation.NoCapacityLimit, cl.Now())
		assert.True(t, ok)
		_, ok = alloc.Attach(eu1, allocation.NoCapacityLimit, lease)
		assert.True(t, ok)
		_, ok = alloc.Attach(jp1, allocation.NoCapacityLimit, lease)
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
		require.NoError(t, alloc.Check())

		cl.Add(time.Second)

		promo := alloc.Expire(cl.Now())
		assert.Len(t, promo, 0)
		require.NoError(t, alloc.Check())

		grants = alloc.Allocate(cl.Now())
		require.Len(t, grants, 2)
		require.NoError(t, alloc.Check())

		m = newGrantMap(grants...)
		assertx.Equal(t, m["a"].Worker, jp1.ID)
		assertx.Equal(t, m["b"].Worker, eu1.ID)
	})

	t.Run("revoke", func(t *testing.T) {
		// Revoke/release functionality

		alloc := allocation.New[string, location.Location, string, location.Location]("id", nil, nil, work, cl.Now())
		assert.Len(t, alloc.Work(), 3)

		// (1) Attach 1 worker and allocate.

		lease := cl.Now().Add(time.Minute)

		us1 := allocation.Worker[string, location.Location]{ID: "us1", Data: us}
		us2 := allocation.Worker[string, location.Location]{ID: "us2", Data: us}

		_, ok := alloc.Attach(us1, allocation.NoCapacityLimit, lease)
		assert.True(t, ok)
		grants := alloc.Allocate(cl.Now())
		assert.Len(t, grants, 3)
		require.NoError(t, alloc.Check())

		// (2) Revoke is possible, even thought it cannot be assigned because
		// two grants for the same work cannot be on the same worker.

		revoked, ok := alloc.Revoke(us1.ID, cl.Now(), grants[0])
		assert.Len(t, revoked, 1)
		assertx.Equal(t, revoked[0].Worker, us1.ID)
		assertx.Equal(t, revoked[0].Unit, grants[0].Unit)
		assertx.Equal(t, revoked[0].State, allocation.Revoked)
		require.NoError(t, alloc.Check())

		grants = alloc.Allocate(cl.Now())
		require.Len(t, grants, 0)
		require.NoError(t, alloc.Check())

		// (2) Attach another worker and it can

		_, ok = alloc.Attach(us2, allocation.NoCapacityLimit, lease)
		assert.True(t, ok)

		grants = alloc.Allocate(cl.Now())
		require.Len(t, grants, 1)
		assertx.Equal(t, grants[0].Worker, us2.ID)
		assertx.Equal(t, grants[0].Unit, grants[0].Unit)
		assertx.Equal(t, grants[0].State, allocation.Allocated)
		require.NoError(t, alloc.Check())

		// (3) Release and the new grant is promoted Active

		promo, ok := alloc.Release(revoked[0], cl.Now())
		assert.True(t, ok)
		assertx.Equal(t, promo.ID, grants[0].ID)
		assertx.Equal(t, promo.Worker, us2.ID)
		assertx.Equal(t, promo.Unit, grants[0].Unit)
		assertx.Equal(t, promo.State, allocation.Active)
		require.NoError(t, alloc.Check())
	})

	t.Run("attach with revoked", func(t *testing.T) {
		alloc := allocation.New[string, location.Location, string, location.Location]("id", nil, nil, work, cl.Now())
		require.Len(t, alloc.Work(), 3)

		// Attach 1 worker with an allocated grant. Revoked counterpart is unknown.

		lease := cl.Now().Add(time.Minute)
		us1 := allocation.Worker[string, location.Location]{ID: "us1", Data: us}

		allocated := allocation.Grant[string, string]{
			ID:         "allocated",
			State:      allocation.Allocated,
			Unit:       "a",
			Worker:     us1.ID,
			Assigned:   cl.Now().Add(-time.Hour),
			Expiration: cl.Now().Add(-time.Second), // expiration time is irrelevant
		}

		attached, ok := alloc.Attach(us1, allocation.NoCapacityLimit, lease, allocated)
		require.True(t, ok)
		require.Len(t, attached.Allocated, 1)
		requirex.Equal(t, attached.Allocated[0].ID, allocated.ID)
		require.Len(t, attached.Active, 0)
		require.Len(t, attached.Revoked, 0)

		// Allocate unassigned work. Allocated grant assigned to the connected worker should not be affected

		grants := alloc.Allocate(cl.Now())
		require.Len(t, grants, 2)
		require.NoError(t, alloc.Check())
		sort.Slice(grants, func(i, j int) bool { return grants[i].Unit < grants[j].Unit })
		requirex.Equal(t, grants[0].Unit, "b")
		requirex.Equal(t, grants[1].Unit, "c")

		require.NoError(t, alloc.Check())

		// Expire grants. Allocated grant should be promoted to active.

		promoted := alloc.Expire(lease.Add(-time.Second))
		require.Len(t, promoted, 1)
		requirex.Equal(t, promoted[0].ID, allocated.ID)
		requirex.Equal(t, promoted[0].State, allocation.Active)

		require.NoError(t, alloc.Check())
	})

	t.Run("update/change-work", func(t *testing.T) {
		// Update functionality

		alloc := allocation.New[string, location.Location, string, location.Location]("id", nil, nil, work, cl.Now())
		assert.Len(t, alloc.Work(), 3)

		// (1) Setup a situation: a, revoked + b, c active.

		lease := cl.Now().Add(time.Minute)

		us1 := allocation.Worker[string, location.Location]{ID: "us1", Data: us}
		us2 := allocation.Worker[string, location.Location]{ID: "us2", Data: us}

		_, ok := alloc.Attach(us1, allocation.NoCapacityLimit, lease)
		assert.True(t, ok)
		_, ok = alloc.Attach(us2, allocation.NoCapacityLimit, lease)
		assert.True(t, ok)

		grants := alloc.Allocate(cl.Now())
		assert.Len(t, grants, 3)
		require.NoError(t, alloc.Check())

		m := newGrantMap(grants...)
		alloc.Revoke(m["a"].Worker, cl.Now(), m["a"])

		assignments := alloc.Assigned(m["a"].Worker)
		require.Len(t, assignments.Active, 0)
		require.Len(t, assignments.Allocated, 0)
		require.Len(t, assignments.Revoked, 1)

		assignments = alloc.Assigned(m["b"].Worker)
		require.Len(t, assignments.Active, 2)
		require.Len(t, assignments.Allocated, 0)
		require.Len(t, assignments.Revoked, 0)

		// (2) Update work to remove b and add d. Revoke status of a is preserved.

		upd := []allocation.Work[string, location.Location]{
			{Unit: "a", Load: 20, Data: us},
			{Unit: "c", Load: 10, Data: eu},
			{Unit: "d", Load: 10, Data: eu},
		}

		cl.Add(time.Second)

		alloc2, rejects := allocation.Update(alloc, nil, nil, upd, cl.Now())
		require.Len(t, rejects, 1)
		assertx.Equal(t, rejects[0].Unit, "b")
		require.NoError(t, alloc2.Check())

		assignments = alloc2.Assigned(m["a"].Worker)
		require.Len(t, assignments.Active, 0)
		require.Len(t, assignments.Allocated, 0)
		require.Len(t, assignments.Revoked, 1)

		assignments = alloc2.Assigned(m["b"].Worker)
		require.Len(t, assignments.Active, 1)
		require.Len(t, assignments.Allocated, 0)
		require.Len(t, assignments.Revoked, 0)

		grants = alloc2.Allocate(cl.Now())
		require.Len(t, grants, 2)
		require.NoError(t, alloc2.Check())

		m = newGrantMap(grants...)
		assertx.Equal(t, m["d"].Unit, "d")
		assertx.Equal(t, m["d"].State, allocation.Active)
		assertx.Equal(t, m["a"].Unit, "a")
		assertx.Equal(t, m["a"].State, allocation.Allocated)
	})

	t.Run("update/add-rules", func(t *testing.T) {
		// Update functionality

		region := allocation.NewPreference("region-affinity", 5, hasRegionAffinity)

		alloc := allocation.New[string, location.Location, string, location.Location]("id", slicex.New(region), nil, work, cl.Now())
		assert.Len(t, alloc.Work(), 3)

		// (1) Setup a situation: a, b, c assigned to regional preference

		lease := cl.Now().Add(time.Minute)

		us := allocation.Worker[string, location.Location]{ID: "us", Data: us}
		eu := allocation.Worker[string, location.Location]{ID: "eu", Data: eu}

		_, ok := alloc.Attach(us, allocation.NoCapacityLimit, lease)
		assert.True(t, ok)
		_, ok = alloc.Attach(eu, allocation.NoCapacityLimit, lease)
		assert.True(t, ok)

		grants := alloc.Allocate(cl.Now())
		assert.Len(t, grants, 3)
		require.NoError(t, alloc.Check())

		assert.Len(t, alloc.Assigned(us.ID).Active, 2)
		assert.Len(t, alloc.Assigned(eu.ID).Active, 1)

		// (2) Update allocation to ban the eu region for workers

		cl.Add(time.Second)

		alloc2, rejects := allocation.Update(alloc, slicex.New(region, newRegionBan(eu.Data.Region)), nil, work, cl.Now().Add(10*time.Second))

		require.Len(t, rejects, 1)
		assertx.Equal(t, rejects[0].Unit, "c")
		require.NoError(t, alloc2.Check())

		_, ok = alloc2.Release(rejects[0], cl.Now()) // Release rejected grant
		assert.False(t, ok)

		grants = alloc2.Allocate(cl.Now())
		assert.Len(t, grants, 1)
		require.NoError(t, alloc2.Check())

		assert.Len(t, alloc2.Assigned(us.ID).Active, 3)
		assert.Len(t, alloc2.Assigned(eu.ID).Active, 0)
	})

	t.Run("load-balance/base-case-two-workers", func(t *testing.T) {
		work := []allocation.Work[string, location.Location]{
			{Unit: "1", Load: 10, Data: us},
			{Unit: "2", Load: 10, Data: us},
		}

		alloc := allocation.New[string, location.Location, string, location.Location]("id", nil, nil, work, cl.Now())

		lease := cl.Now().Add(time.Minute)

		w1 := allocation.Worker[string, location.Location]{ID: "w1", Data: us}
		w2 := allocation.Worker[string, location.Location]{ID: "w2", Data: us}

		_, ok := alloc.Attach(w1, allocation.NoCapacityLimit, lease)
		assert.True(t, ok)

		grants := alloc.Allocate(cl.Now())
		assert.Len(t, grants, 2)

		m := newGrantMap(grants...)
		assertx.Equal(t, m["1"].Worker, w1.ID)
		assertx.Equal(t, m["2"].Worker, w1.ID)

		_, ok = alloc.Attach(w2, allocation.NoCapacityLimit, lease)
		assert.True(t, ok)

		grants = alloc.Allocate(cl.Now())
		assert.Len(t, grants, 0) // all work has already been assigned to w1

		// LoadBalance should move a grant from w1 to w2.
		move, _, ok := alloc.LoadBalance(cl.Now(), nil)
		assert.True(t, ok)
		assertx.Equal(t, move.From.Worker, w1.ID)
		assertx.Equal(t, move.From.State, allocation.Revoked)
		assertx.Equal(t, move.To.Worker, w2.ID)
		assertx.Equal(t, move.To.State, allocation.Allocated)
		require.NoError(t, alloc.Check())

		_, _, ok = alloc.LoadBalance(cl.Now(), nil)
		assert.False(t, ok) // No movement
	})

	t.Run("load-balance/canary-capacity-limit", func(t *testing.T) {
		work := []allocation.Work[string, location.Location]{
			{Unit: "1", Load: 10, Data: us},
			{Unit: "2", Load: 10, Data: us},
			{Unit: "3", Load: 10, Data: us},
			{Unit: "4", Load: 10, Data: us},
			{Unit: "5", Load: 10, Data: us},
			{Unit: "6", Load: 10, Data: us},
			{Unit: "7", Load: 10, Data: us},
			{Unit: "8", Load: 10, Data: us},
			{Unit: "9", Load: 10, Data: us},
			{Unit: "10", Load: 10, Data: us},
			{Unit: "11", Load: 10, Data: us},
			{Unit: "12", Load: 10, Data: us},
			{Unit: "13", Load: 10, Data: us},
			{Unit: "14", Load: 10, Data: us},
			{Unit: "15", Load: 10, Data: us},
			{Unit: "16", Load: 10, Data: us},
			{Unit: "leader", Load: 50, Data: us},
		}

		alloc := allocation.New[string, location.Location, string, location.Location]("id", nil, nil, work, cl.Now())

		// (1) Start with 5 workers. Allocate to distribute work evenly.

		lease := cl.Now().Add(time.Minute)

		w1 := allocation.Worker[string, location.Location]{ID: "w1", Data: us}
		w2 := allocation.Worker[string, location.Location]{ID: "w2", Data: us}
		w3 := allocation.Worker[string, location.Location]{ID: "w3", Data: us}
		w4 := allocation.Worker[string, location.Location]{ID: "w4", Data: us}
		w5 := allocation.Worker[string, location.Location]{ID: "w5", Data: us}

		_, ok := alloc.Attach(w1, allocation.NoCapacityLimit, lease)
		assert.True(t, ok)
		_, ok = alloc.Attach(w2, allocation.NoCapacityLimit, lease)
		assert.True(t, ok)
		_, ok = alloc.Attach(w3, allocation.NoCapacityLimit, lease)
		assert.True(t, ok)
		_, ok = alloc.Attach(w4, allocation.NoCapacityLimit, lease)
		assert.True(t, ok)
		_, ok = alloc.Attach(w5, allocation.NoCapacityLimit, lease)
		assert.True(t, ok)

		grants := alloc.Allocate(cl.Now())
		assert.Len(t, grants, 17) // all work has been assigned

		w1Load, _ := alloc.LoadByWorker(w1.ID)
		w2Load, _ := alloc.LoadByWorker(w2.ID)
		w3Load, _ := alloc.LoadByWorker(w3.ID)
		w4Load, _ := alloc.LoadByWorker(w4.ID)
		w5Load, _ := alloc.LoadByWorker(w5.ID)
		assert.Subset(t, []allocation.Load{w1Load.Load, w2Load.Load, w3Load.Load, w4Load.Load, w5Load.Load}, []allocation.Load{50, 40, 40, 40, 40})

		// (2) Attach two canary workers. Load balance should assign work.

		c := allocation.Worker[string, location.Location]{ID: "c", Data: us}
		b := allocation.Worker[string, location.Location]{ID: "b", Data: us}

		_, ok = alloc.Attach(c, 10, lease)
		assert.True(t, ok)
		_, ok = alloc.Attach(b, 10, lease)
		assert.True(t, ok)

		loads, ok := loadBalanceIterations(t, alloc, nil, 2, cl.Now())
		require.True(t, ok)
		assert.Subset(t, intrinsicLoads(loads, "w1", "w2", "w3", "w4", "w5"), []allocation.Load{50, 40, 40, 30, 30})
		assert.Subset(t, intrinsicLoads(loads, "c", "b"), []allocation.Load{10, 10})
	})

	t.Run("load-balance/skew-two-workers", func(t *testing.T) {
		work := []allocation.Work[string, location.Location]{
			{Unit: "a", Load: 20, Data: us},
			{Unit: "b", Load: 10, Data: us},
			{Unit: "c", Load: 10, Data: us},
		}

		alloc := allocation.New[string, location.Location, string, location.Location]("id", nil, nil, work, cl.Now())
		assert.Len(t, alloc.Work(), 3)

		// (1) Start with 1 worker allocated with 3 grants.

		lease := cl.Now().Add(time.Minute)

		us1 := allocation.Worker[string, location.Location]{ID: "us1", Data: us}

		_, ok := alloc.Attach(us1, allocation.NoCapacityLimit, lease)
		assert.True(t, ok)
		grants := alloc.Allocate(cl.Now())
		assert.Len(t, grants, 3)

		// (2) Add another worker and load-balance. Expect highest load work unit "a" to move

		us2 := allocation.Worker[string, location.Location]{ID: "us2", Data: us}

		_, ok = alloc.Attach(us2, allocation.NoCapacityLimit, lease)
		assert.True(t, ok)

		// (10, 10), (20)
		move, diff, ok := alloc.LoadBalance(cl.Now(), nil)
		assert.True(t, ok)
		assertx.Equal(t, move.From.Worker, us1.ID)
		assertx.Equal(t, move.From.Unit, "a")
		assertx.Equal(t, move.From.State, allocation.Revoked)
		assertx.Equal(t, move.To.Worker, us2.ID)
		assertx.Equal(t, move.To.Unit, "a")
		assertx.Equal(t, move.To.State, allocation.Allocated)
		assertx.Equal(t, diff, allocation.AdjustedLoad{Load: -3})
		require.NoError(t, alloc.Check())

		_, _, ok = alloc.LoadBalance(cl.Now(), nil)
		assert.False(t, ok) // No movement
		require.NoError(t, alloc.Check())
	})

	t.Run("load-balance/skew-three-workers", func(t *testing.T) {
		work := []allocation.Work[string, location.Location]{
			{Unit: "1", Load: 10, Data: us},
			{Unit: "2", Load: 10, Data: us},
			{Unit: "3", Load: 50, Data: us},
		}

		alloc := allocation.New[string, location.Location, string, location.Location]("id", nil, nil, work, cl.Now())

		lease := cl.Now().Add(time.Minute)

		// (1) Start with 2 workers. Allocate should distribute work evenly among the two workers

		w1 := allocation.Worker[string, location.Location]{ID: "w1", Data: us}
		w2 := allocation.Worker[string, location.Location]{ID: "w2", Data: us}

		_, ok := alloc.Attach(w1, allocation.NoCapacityLimit, lease)
		assert.True(t, ok)
		_, ok = alloc.Attach(w2, allocation.NoCapacityLimit, lease)
		assert.True(t, ok)

		grants := alloc.Allocate(cl.Now())
		assert.Len(t, grants, 3)
		w1Load, _ := alloc.LoadByWorker(w1.ID)
		w2Load, _ := alloc.LoadByWorker(w2.ID)
		assert.Subset(t, []allocation.Load{w1Load.Load, w2Load.Load}, []allocation.Load{50, 20})

		// (2) Add a worker. Load balance should assign work to it and stop.

		w3 := allocation.Worker[string, location.Location]{ID: "w3", Data: us}
		_, ok = alloc.Attach(w3, allocation.NoCapacityLimit, lease)
		assert.True(t, ok)

		loads, ok := loadBalanceIterations(t, alloc, nil, 1, cl.Now())
		require.True(t, ok)
		assert.Subset(t, intrinsicLoads(loads, "w1", "w2", "w3"), []allocation.Load{50, 10, 10})

		require.NoError(t, alloc.Check())
	})

	t.Run("load-balance/skew-six-workers", func(t *testing.T) {
		work := []allocation.Work[string, location.Location]{
			{Unit: "1", Load: 10, Data: us},
			{Unit: "2", Load: 10, Data: us},
			{Unit: "3", Load: 10, Data: us},
			{Unit: "4", Load: 10, Data: us},
			{Unit: "5", Load: 50, Data: us},
		}

		alloc := allocation.New[string, location.Location, string, location.Location]("id", nil, nil, work, cl.Now())

		lease := cl.Now().Add(time.Minute)

		// (1) Start with 2 workers. Allocate should distribute work evenly.

		w1 := allocation.Worker[string, location.Location]{ID: "w1", Data: us}
		w2 := allocation.Worker[string, location.Location]{ID: "w2", Data: us}

		_, ok := alloc.Attach(w1, allocation.NoCapacityLimit, lease)
		assert.True(t, ok)
		_, ok = alloc.Attach(w2, allocation.NoCapacityLimit, lease)
		assert.True(t, ok)

		grants := alloc.Allocate(cl.Now())
		assert.Len(t, grants, 5)

		// (10, 10, 10, 10), (50)
		w1Load, _ := alloc.LoadByWorker(w1.ID)
		w2Load, _ := alloc.LoadByWorker(w2.ID)
		assert.Subset(t, []allocation.Load{w1Load.Load, w2Load.Load}, []allocation.Load{40, 50})

		// (2) Add another worker. Load balance should assign work to it and stop.

		w3 := allocation.Worker[string, location.Location]{ID: "w3", Data: us}
		_, ok = alloc.Attach(w3, allocation.NoCapacityLimit, lease)
		assert.True(t, ok)

		loads, ok := loadBalanceIterations(t, alloc, nil, 2, cl.Now())
		require.True(t, ok)
		assert.Subset(t, intrinsicLoads(loads, "w1", "w2", "w3"), []allocation.Load{20, 20, 50})

		// (3) Add another worker. Load balance should assign work to it and stop.

		w4 := allocation.Worker[string, location.Location]{ID: "w4", Data: us}
		_, ok = alloc.Attach(w4, allocation.NoCapacityLimit, lease)
		assert.True(t, ok)

		loads, ok = loadBalanceIterations(t, alloc, nil, 1, cl.Now())
		require.True(t, ok)
		assert.Subset(t, intrinsicLoads(loads, "w1", "w2", "w3", "w4"), []allocation.Load{10, 10, 20, 50})

		// (4) Add 2 workers. Load balance should assign work to one of them and stop.

		w5 := allocation.Worker[string, location.Location]{ID: "w5", Data: us}
		w6 := allocation.Worker[string, location.Location]{ID: "w6", Data: us}
		_, ok = alloc.Attach(w5, allocation.NoCapacityLimit, lease)
		assert.True(t, ok)
		_, ok = alloc.Attach(w6, allocation.NoCapacityLimit, lease)
		assert.True(t, ok)

		loads, ok = loadBalanceIterations(t, alloc, nil, 1, cl.Now())
		require.True(t, ok)
		assert.Subset(t, intrinsicLoads(loads, "w1", "w2", "w3", "w4", "w5", "w6"), []allocation.Load{0, 10, 10, 10, 10, 50})

		require.NoError(t, alloc.Check())
	})

	t.Run("load-balance/region-affinity", func(t *testing.T) {
		region := allocation.NewPreference("region-affinity", 20, hasRegionAffinity)

		alloc := allocation.New[string, location.Location, string, location.Location]("id", slicex.New(region), nil, work, cl.Now())
		assert.Len(t, alloc.Work(), 3)

		// (1) Start with 1 us worker allocated with 3 grants.

		lease := cl.Now().Add(time.Minute)

		us1 := allocation.Worker[string, location.Location]{ID: "us1", Data: us}

		_, ok := alloc.Attach(us1, allocation.NoCapacityLimit, lease)
		assert.True(t, ok)
		grants := alloc.Allocate(cl.Now())
		assert.Len(t, grants, 3)

		us1Load, _ := alloc.LoadByWorker(us1.ID)
		assertx.Equal(t, us1Load, allocation.AdjustedLoad{Load: 40, Place: 20})

		// (2) Add eu worker and load-balance. Expect eu work "c" to move to region-local worker.

		eu1 := allocation.Worker[string, location.Location]{ID: "eu1", Data: eu}

		_, ok = alloc.Attach(eu1, allocation.NoCapacityLimit, lease)
		assert.True(t, ok)
		move, diff, ok := alloc.LoadBalance(cl.Now(), nil)
		assert.True(t, ok)
		assertx.Equal(t, move.From.Worker, us1.ID)
		assertx.Equal(t, move.From.Unit, "c")
		assertx.Equal(t, move.From.State, allocation.Revoked)
		assertx.Equal(t, move.To.Worker, eu1.ID)
		assertx.Equal(t, move.To.Unit, "c")
		assertx.Equal(t, move.To.State, allocation.Allocated)
		assertx.Equal(t, diff, allocation.AdjustedLoad{Load: -1, Place: -20})
		require.NoError(t, alloc.Check())

		us1Load, _ = alloc.LoadByWorker(us1.ID)
		eu1Load, _ := alloc.LoadByWorker(eu1.ID)
		assertx.Equal(t, us1Load, allocation.AdjustedLoad{Load: 30, Place: 0})
		assertx.Equal(t, eu1Load, allocation.AdjustedLoad{Load: 10, Place: 0})

		_, _, ok = alloc.LoadBalance(cl.Now(), nil)
		assert.False(t, ok) // No movement

		// (3) Release and the allocated grant is promoted Active

		promo, ok := alloc.Release(move.From, cl.Now())
		assert.True(t, ok)
		assertx.Equal(t, promo.ID, move.To.ID)
		assertx.Equal(t, promo.Worker, eu1.ID)
		assertx.Equal(t, promo.Unit, "c")
		assertx.Equal(t, promo.State, allocation.Active)
		assertx.Equal(t, promo.Assigned, cl.Now())
		require.NoError(t, alloc.Check())
	})

	t.Run("load-balance/region-misplaced-correction", func(t *testing.T) {
		work := []allocation.Work[string, location.Location]{
			{Unit: "1", Load: 10, Data: us},
			{Unit: "2", Load: 10, Data: us},
			{Unit: "3", Load: 10, Data: us},
			{Unit: "4", Load: 10, Data: us},
			{Unit: "5", Load: 10, Data: us},
			{Unit: "6", Load: 10, Data: us},
			{Unit: "7", Load: 10, Data: us},
			{Unit: "8", Load: 10, Data: us},
		}
		region := allocation.NewPreference("region-affinity", 20, hasRegionAffinity)
		alloc := allocation.New[string, location.Location, string, location.Location]("id", slicex.New(region), nil, work, cl.Now())
		assert.Len(t, alloc.Work(), 8)

		lease := cl.Now().Add(time.Minute)
		eu1 := allocation.Worker[string, location.Location]{ID: "eu1", Data: eu}

		// (1) Start with 1 eu worker. Allocate all us grants to it. Expect misplacement.

		_, ok := alloc.Attach(eu1, allocation.NoCapacityLimit, lease)
		assert.True(t, ok)
		grants := alloc.Allocate(cl.Now())
		assert.Len(t, grants, 8)
		eu1Load, _ := alloc.LoadByWorker(eu1.ID)
		assertx.Equal(t, eu1Load, allocation.AdjustedLoad{Load: 80, Place: 20 * 8})

		// (2) Attach us worker. Load balance should move all work from eu worker, reducing placement penalty to zero.

		us1 := allocation.Worker[string, location.Location]{ID: "us1", Data: us}
		_, ok = alloc.Attach(us1, allocation.NoCapacityLimit, lease)
		assert.True(t, ok)

		loads, ok := loadBalanceIterations(t, alloc, nil, 8, cl.Now()) // needs 8 iterations to move 8 grants
		require.True(t, ok)
		assertx.Equal(t, loads["us1"].Load, 80)
		assertx.Equal(t, loads["eu1"].Total(), 0) // eu has zero load

		// (3) Attach another us worker. Load balance should distribute work between us workers.

		us2 := allocation.Worker[string, location.Location]{ID: "us2", Data: us}
		_, ok = alloc.Attach(us2, allocation.NoCapacityLimit, lease)
		assert.True(t, ok)

		loads, ok = loadBalanceIterations(t, alloc, nil, 4, cl.Now())
		require.True(t, ok)
		assertx.Equal(t, loads["us1"].Load, 40)
		assertx.Equal(t, loads["us2"].Load, 40)
		assertx.Equal(t, loads["eu1"].Total(), 0) // unchanged
		require.NoError(t, alloc.Check())
	})

	t.Run("load-balance/skew-regional-balance", func(t *testing.T) {
		work := []allocation.Work[string, location.Location]{
			{Unit: "1", Load: 10, Data: us},
			{Unit: "2", Load: 10, Data: us},
			{Unit: "3", Load: 10, Data: us},
			{Unit: "4", Load: 10, Data: us},
			{Unit: "5", Load: 10, Data: us},
			{Unit: "6", Load: 10, Data: us},
			{Unit: "7", Load: 10, Data: us},
			{Unit: "8", Load: 10, Data: us},
			{Unit: "9", Load: 10, Data: eu},
			{Unit: "10", Load: 10, Data: eu},
			{Unit: "11", Load: 10, Data: eu},
			{Unit: "12", Load: 10, Data: eu},
			{Unit: "leader", Load: 50, Data: us},
		}

		region := allocation.NewPreference("region-affinity", 20, hasRegionAffinity)
		alloc := allocation.New[string, location.Location, string, location.Location]("id", slicex.New(region), nil, work, cl.Now())
		assert.Len(t, alloc.Work(), 13)

		// (1) Start with 2 us + 2 eu workers, allocated with 13 grants.

		lease := cl.Now().Add(time.Minute)

		us1 := allocation.Worker[string, location.Location]{ID: "us1", Data: us}
		us2 := allocation.Worker[string, location.Location]{ID: "us2", Data: us}
		eu1 := allocation.Worker[string, location.Location]{ID: "eu1", Data: eu}
		eu2 := allocation.Worker[string, location.Location]{ID: "eu2", Data: eu}
		_, ok := alloc.Attach(us1, allocation.NoCapacityLimit, lease)
		assert.True(t, ok)
		_, ok = alloc.Attach(us2, allocation.NoCapacityLimit, lease)
		assert.True(t, ok)
		_, ok = alloc.Attach(eu1, allocation.NoCapacityLimit, lease)
		assert.True(t, ok)
		_, ok = alloc.Attach(eu2, allocation.NoCapacityLimit, lease)
		assert.True(t, ok)

		grants := alloc.Allocate(cl.Now())
		assert.Len(t, grants, 13)

		// us: (50, 10, 10), (10, 10, 10, 10, 10, 10)
		// eu: (10, 10), (10, 10)

		us1Load, _ := alloc.LoadByWorker(us1.ID)
		us2Load, _ := alloc.LoadByWorker(us2.ID)
		assert.Subset(t, []allocation.Load{us1Load.Load, us2Load.Load}, []allocation.Load{70, 60})

		eu1Load, _ := alloc.LoadByWorker(eu1.ID)
		eu2Load, _ := alloc.LoadByWorker(eu2.ID)
		assert.Subset(t, []allocation.Load{eu1Load.Load, eu2Load.Load}, []allocation.Load{20, 20})

		// (2) Add 1 us + 1 eu worker. Load balance should distribute work evenly in each region.

		us3 := allocation.Worker[string, location.Location]{ID: "us3", Data: us}
		eu3 := allocation.Worker[string, location.Location]{ID: "eu3", Data: eu}
		_, ok = alloc.Attach(us3, allocation.NoCapacityLimit, lease)
		assert.True(t, ok)
		_, ok = alloc.Attach(eu3, allocation.NoCapacityLimit, lease)
		assert.True(t, ok)

		loads, ok := loadBalanceIterations(t, alloc, nil, 4, cl.Now())
		require.True(t, ok)

		// us: (50), (10, 10, 10, 10), (10, 10, 10, 10)
		// eu: (10, 10), (10), (10)
		assert.Subset(t, intrinsicLoads(loads, "us1", "us2", "us3"), []allocation.Load{50, 40, 40})
		assert.Subset(t, intrinsicLoads(loads, "eu1", "eu2", "eu3"), []allocation.Load{20, 10, 10})
		_, adj := alloc.Load()
		assertx.Equal(t, adj.Place, 0) // No placement penalty

		// (3) Add 2 us workers. Load balance should distribute work among us workers.

		us4 := allocation.Worker[string, location.Location]{ID: "us4", Data: us}
		us5 := allocation.Worker[string, location.Location]{ID: "us5", Data: us}
		_, ok = alloc.Attach(us4, allocation.NoCapacityLimit, lease)
		assert.True(t, ok)
		_, ok = alloc.Attach(us5, allocation.NoCapacityLimit, lease)
		assert.True(t, ok)

		loads, ok = loadBalanceIterations(t, alloc, nil, 4, cl.Now())
		require.True(t, ok)

		// us: (50), (10, 10), (10, 10), (10, 10), (10, 10)
		// eu: (10, 10), (10), (10)
		assert.Subset(t, intrinsicLoads(loads, "us1", "us2", "us3", "us4", "us5"), []allocation.Load{50, 20, 20, 20, 20})
		assert.Subset(t, intrinsicLoads(loads, "eu1", "eu2", "eu3"), []allocation.Load{20, 10, 10}) // unchanged

		_, adj = alloc.Load()
		assertx.Equal(t, adj.Place, 0) // No placement penalty
		require.NoError(t, alloc.Check())
	})

	t.Run("load-balance/wrong-region-placement", func(t *testing.T) {
		work := []allocation.Work[string, location.Location]{
			{Unit: "1", Load: 10, Data: us},
			{Unit: "2", Load: 10, Data: us},
			{Unit: "3", Load: 10, Data: us},
			{Unit: "4", Load: 10, Data: us},
			{Unit: "5", Load: 10, Data: us},
			{Unit: "6", Load: 10, Data: us},
			{Unit: "7", Load: 10, Data: us},
			{Unit: "8", Load: 10, Data: us},
			{Unit: "9", Load: 10, Data: us},
			{Unit: "10", Load: 10, Data: us},
			{Unit: "11", Load: 10, Data: us},
			{Unit: "12", Load: 10, Data: us},
			{Unit: "13", Load: 10, Data: us},
			{Unit: "14", Load: 10, Data: us},
			{Unit: "15", Load: 10, Data: us},
			{Unit: "16", Load: 10, Data: us},
			{Unit: "leader", Load: 50, Data: us},
		}

		region := allocation.NewPreference("region-affinity", 20, hasRegionAffinity)
		alloc := allocation.New[string, location.Location, string, location.Location]("id", slicex.New(region), nil, work, cl.Now())
		assert.Len(t, alloc.Work(), 17)

		// (1) Start with 1 us worker allocated with 17 grants.

		lease := cl.Now().Add(time.Minute)

		us1 := allocation.Worker[string, location.Location]{ID: "us1", Data: us}

		_, ok := alloc.Attach(us1, allocation.NoCapacityLimit, lease)
		assert.True(t, ok)
		grants := alloc.Allocate(cl.Now())
		assert.Len(t, grants, 17)

		us1load, _ := alloc.LoadByWorker(us1.ID)
		assertx.Equal(t, us1load.Load, 210)

		// (2) Add eu worker. Ensure that load-balance does not move the leader off the overloaded us worker to eu.

		eu1 := allocation.Worker[string, location.Location]{ID: "eu1", Data: eu}

		_, ok = alloc.Attach(eu1, allocation.NoCapacityLimit, lease)
		assert.True(t, ok)

		loads, ok := loadBalanceIterations(t, alloc, slicex.NewSet("leader"), 0, cl.Now())
		require.True(t, ok)

		assertx.Equal(t, loads["us1"].Load, 210)
		assertx.Equal(t, loads["eu1"].Total(), 0)
		require.NoError(t, alloc.Check())
	})

	t.Run("load-balance/too-many-workers", func(t *testing.T) {
		work := []allocation.Work[string, location.Location]{
			{Unit: "1", Load: 10, Data: us},
			{Unit: "2", Load: 10, Data: us},
		}

		alloc := allocation.New[string, location.Location, string, location.Location]("id", nil, nil, work, cl.Now())
		assert.Len(t, alloc.Work(), 2)

		// (1) Start with 1 us worker allocated with 2 grants.

		lease := cl.Now().Add(time.Minute)

		us1 := allocation.Worker[string, location.Location]{ID: "us1", Data: us}

		_, ok := alloc.Attach(us1, allocation.NoCapacityLimit, lease)
		assert.True(t, ok)
		grants := alloc.Allocate(cl.Now())
		assert.Len(t, grants, 2)

		us1load, _ := alloc.LoadByWorker(us1.ID)
		assertx.Equal(t, us1load.Load, 20)

		// (2) Add 3 workers. Load balance should move one grant and stop.

		us2 := allocation.Worker[string, location.Location]{ID: "us2", Data: us}
		us3 := allocation.Worker[string, location.Location]{ID: "us3", Data: us}
		us4 := allocation.Worker[string, location.Location]{ID: "us4", Data: us}

		_, ok = alloc.Attach(us2, allocation.NoCapacityLimit, lease)
		assert.True(t, ok)
		_, ok = alloc.Attach(us3, allocation.NoCapacityLimit, lease)
		assert.True(t, ok)
		_, ok = alloc.Attach(us4, allocation.NoCapacityLimit, lease)
		assert.True(t, ok)

		loads, ok := loadBalanceIterations(t, alloc, nil, 1, cl.Now())
		require.True(t, ok)
		assert.Subset(t, intrinsicLoads(loads, "us1"), []allocation.Load{10}) // lost 1 grant to any of the new workers
		assert.Subset(t, intrinsicLoads(loads, "us2", "us3", "us4"), []allocation.Load{10, 0, 0})
		require.NoError(t, alloc.Check())
	})
}

func newGrantMap[T, K comparable](list ...allocation.Grant[T, K]) map[T]allocation.Grant[T, K] {
	return mapx.New(list, func(v allocation.Grant[T, K]) T {
		return v.Unit
	})
}

func loadBalanceIterations(t *testing.T, alloc *allocation.Allocation[string, location.Location, string, location.Location], ignore map[string]bool, numItr int, now time.Time) (map[string]allocation.AdjustedLoad, bool) {
	for i := 0; i < numItr; i++ {
		var move allocation.Move[string, string]
		var ok bool
		if move, _, ok = alloc.LoadBalance(now, ignore); !ok {
			return nil, false // stop before the expected iterations
		}
		// release to update grant state to active
		if promo, ok := alloc.Release(move.From, now); ok {
			require.Equal(t, promo.State, allocation.Active)
		} else {
			require.Fail(t, "failed to release %v", move.From)
		}
	}

	if _, _, ok := alloc.LoadBalance(now, ignore); ok {
		return nil, false // load balance does not stop at (n+1) iterations
	}

	ret := map[string]allocation.AdjustedLoad{}
	for _, w := range alloc.Workers() {
		ret[w.Instance.ID], _ = alloc.LoadByWorker(w.Instance.ID)
	}
	return ret, true
}

func intrinsicLoads(loads map[string]allocation.AdjustedLoad, keys ...string) []allocation.Load {
	filtered := mapx.FilterKeys(loads, func(s string) bool {
		return slices.Contains(keys, s)
	})
	return mapx.MapToSlice(filtered, func(k string, v allocation.AdjustedLoad) allocation.Load {
		return v.Load
	})
}
