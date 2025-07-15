package leader

import (
	"slices"
	"time"

	"go.atoms.co/splitter/lib/service/location"
	"go.atoms.co/slicex"
	"go.atoms.co/splitter/pkg/allocation"
	"go.atoms.co/splitter/pkg/core"
	"go.atoms.co/splitter/pkg/model"
)

type (
	Allocation = allocation.Allocation[model.QualifiedServiceName, []location.Location, location.InstanceID, model.Instance]
	Placement  = allocation.Placement[model.QualifiedServiceName, []location.Location, location.InstanceID, model.Instance]
	Grant      = allocation.Grant[model.QualifiedServiceName, location.InstanceID]
	Worker     = allocation.Worker[location.InstanceID, model.Instance]
	Work       = allocation.Work[model.QualifiedServiceName, []location.Location]
)

var (
	regionAffinity = allocation.NewPreference("region-affinity", 20, HasRegionAffinity)
)

func HasRegionAffinity(worker Worker, work Work) bool {
	return len(work.Data) == 0 || slices.ContainsFunc(work.Data, func(l location.Location) bool {
		return worker.Data.Location().Region == l.Region
	})
}

func newAllocation(id location.InstanceID, snapshot core.Snapshot, activation time.Time) *Allocation {
	return allocation.New(id, findPlacements(snapshot), nil, findWork(snapshot), activation)
}

func updateAllocation(alloc *Allocation, snapshot core.Snapshot, activation time.Time) (*Allocation, []Grant) {
	return allocation.Update(alloc, findPlacements(snapshot), nil, findWork(snapshot), activation)
}

// control handles placement control.
type serviceRegion struct {
	service model.QualifiedServiceName
	region  model.Region
}

type Control struct {
	Banned map[serviceRegion]bool
}

func NewControl(snapshot core.Snapshot) *Control {
	banned := make(map[serviceRegion]bool)
	for _, state := range snapshot.Tenants() {
		t := state.Tenant().Tenant().Operational().BannedRegions()

		for _, info := range state.Services() {
			s := info.Service().Operational().BannedRegions()

			for _, b := range [][]model.Region{t, s} {
				for _, region := range b {
					banned[serviceRegion{service: info.Name(), region: region}] = true
				}
			}
		}
	}
	return &Control{Banned: banned}
}

func (c *Control) ID() allocation.Rule {
	return "control"
}

func (c *Control) TryPlace(worker Worker, work Work) (allocation.Load, bool) {
	if c.Banned[serviceRegion{service: work.Unit, region: worker.Data.Location().Region}] {
		return 0, false
	}
	return 0, true
}

func findPlacements(snapshot core.Snapshot) []Placement {
	return slicex.New[Placement](regionAffinity, NewControl(snapshot))
}

func findWork(snapshot core.Snapshot) []Work {
	var ret []Work
	for _, tenant := range snapshot.Tenants() {
		for _, info := range tenant.Services() {
			cfg := info.Info().Service().Config()

			rs := cfg.Regions()
			if len(rs) == 0 {
				rs = append(rs, cfg.Region())
			}

			ls := slicex.Map(rs, func(r model.Region) location.Location {
				return location.Location{Region: r}
			})

			w := Work{
				Unit: info.Info().Name(),
				Data: ls,
				Load: 10 + 4*allocation.Load(len(info.Domains())), // weight by #domains
			}
			ret = append(ret, w)
		}

	}
	return ret
}

func fromGrant(worker location.InstanceID, g core.Grant) Grant {
	return allocation.NewGrant(g.ID(), allocation.Active, allocation.None, g.Service(), worker, g.Assigned(), g.Lease())
}

func toGrant(g Grant) core.Grant {
	return core.NewGrant(g.ID, g.Unit, g.Expiration, g.Assigned)
}

func byWorker(grants ...Grant) map[location.InstanceID][]Grant {
	m := map[location.InstanceID][]Grant{}
	for _, grant := range grants {
		m[grant.Worker] = append(m[grant.Worker], grant)
	}
	return m
}
