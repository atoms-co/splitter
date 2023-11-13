package leader

import (
	"go.atoms.co/splitter/lib/service/location"
	"go.atoms.co/slicex"
	"go.atoms.co/splitter/pkg/allocation"
	"go.atoms.co/splitter/pkg/core"
	"go.atoms.co/splitter/pkg/model"
	"time"
)

type (
	Allocation = allocation.Allocation[model.QualifiedServiceName]
	Grant      = allocation.Grant[model.QualifiedServiceName]
	Work       = allocation.Work[model.QualifiedServiceName]
)

var (
	regionAffinity = allocation.NewPreference(allocation.RegionAffinityRule, 20, allocation.HasRegionAffinity[model.QualifiedServiceName])
)

func newAllocation(id location.InstanceID, snapshot core.Snapshot, activation time.Time) *Allocation {
	return allocation.New(id, slicex.New(regionAffinity), nil, findWork(snapshot), activation)
}

func updateAllocation(alloc *Allocation, snapshot core.Snapshot, activation time.Time) (*Allocation, []Grant) {
	return allocation.Update(alloc, slicex.New(regionAffinity), nil, findWork(snapshot), activation)
}

func findWork(snapshot core.Snapshot) []Work {
	var ret []Work
	for _, tenant := range snapshot.Tenants() {
		for _, info := range tenant.Services() {
			r, _ := info.Info().Service().Region()

			w := Work{
				Unit:     info.Info().Name(),
				Location: location.Location{Region: r},
				Load:     10,
			}
			ret = append(ret, w)
		}

	}
	return ret
}

func toGrant(g Grant) core.Grant {
	return core.NewGrant(g.ID, g.Unit, g.Expiration, g.Assigned)
}
