package core

import (
	"go.atoms.co/lib/mapx"
	"go.atoms.co/slicex"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pb/private"
	"fmt"
	"time"
)

// Assignment holds a coordinator instance and its active grants, potentially none.
type Assignment struct {
	Worker model.Instance
	Grants []Grant
}

func ParseClusterAssignment(assignment *internal_v1.ClusterMessage_Assignment) Assignment {
	return Assignment{
		Worker: model.WrapInstance(assignment.GetWorker()),
		Grants: slicex.Map(assignment.GetGrants(), WrapGrant),
	}
}

func (a Assignment) ToProto() *internal_v1.ClusterMessage_Assignment {
	return &internal_v1.ClusterMessage_Assignment{
		Worker: model.UnwrapInstance(a.Worker),
		Grants: slicex.Map(a.Grants, UnwrapGrant),
	}
}

func (a Assignment) String() string {
	return fmt.Sprintf("%v{%v}", a.Worker, a.Grants)
}

// Coordinator holds the worker and assigning service grant of a coordinator.
type Coordinator struct {
	Worker model.Instance
	Grant  Grant
}

func (c Coordinator) String() string {
	return fmt.Sprintf("%v@%v", c.Grant, c.Worker)
}

// Cluster contains information about location of coordinators responsible for services. Immutable.
type Cluster struct {
	id       model.ClusterID
	services map[model.QualifiedServiceName]Coordinator
}

func NewCluster(id model.ClusterID, assignments ...Assignment) *Cluster {
	services := map[model.QualifiedServiceName]Coordinator{}
	for _, a := range assignments {
		for _, grant := range a.Grants {
			services[grant.Service()] = Coordinator{Worker: a.Worker, Grant: grant}
		}
	}
	return &Cluster{id: id, services: services}
}

func UpdateCluster(c *Cluster, add []Assignment, remove []model.QualifiedServiceName, now time.Time) *Cluster {
	ret := &Cluster{
		id:       c.id.Next(now),
		services: mapx.Clone(c.services),
	}

	for _, assignment := range add {
		for _, grant := range assignment.Grants {
			ret.services[grant.Service()] = Coordinator{
				Worker: assignment.Worker,
				Grant:  grant,
			}
		}
	}
	for _, service := range remove {
		delete(ret.services, service)
	}
	return ret
}

func (c *Cluster) ID() model.ClusterID {
	return c.id
}

func (c *Cluster) Workers() []model.Instance {
	return mapx.Values(mapx.Map(c.services, func(k model.QualifiedServiceName, v Coordinator) (model.InstanceID, model.Instance) {
		return v.Worker.ID(), v.Worker
	}))
}

func (c *Cluster) Lookup(service model.QualifiedServiceName) (Coordinator, bool) {
	ret, ok := c.services[service]
	return ret, ok
}

func (c *Cluster) Assignments() []Assignment {
	workers := map[model.InstanceID]model.Instance{}
	grants := map[model.InstanceID][]Grant{}

	for _, coordinator := range c.services {
		id := coordinator.Worker.ID()
		workers[id] = coordinator.Worker
		grants[id] = append(grants[id], coordinator.Grant)
	}

	var ret []Assignment
	for id, inst := range workers {
		ret = append(ret, Assignment{Worker: inst, Grants: grants[id]})
	}
	return ret
}

func (c *Cluster) String() string {
	return fmt.Sprintf("%v{%v}", c.id, c.services)
}
