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
	Allocation = allocation.Allocation[model.QualifiedServiceName, location.Location, location.InstanceID, model.Instance]
	Grant      = allocation.Grant[model.QualifiedServiceName, location.InstanceID]
	Worker     = allocation.Worker[location.InstanceID, model.Instance]
	Work       = allocation.Work[model.QualifiedServiceName, location.Location]
)

var (
	regionAffinity = allocation.NewPreference("region-affinity", 20, HasRegionAffinity)
)

func HasRegionAffinity(worker Worker, work Work) bool {
	return work.Data.Region == "" || worker.Data.Location().Region == work.Data.Region
}

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
				Unit: info.Info().Name(),
				Data: location.Location{Region: r},
				Load: 10,
			}
			ret = append(ret, w)
		}

	}
	return ret
}

func toGrant(g Grant) core.Grant {
	return core.NewGrant(g.ID, g.Unit, g.Expiration, g.Assigned)
}
