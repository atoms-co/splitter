package core

import (
	"go.atoms.co/lib/mapx"
	"go.atoms.co/slicex"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pb/private"
	"fmt"
)

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

func (c Assignment) ToProto() *internal_v1.ClusterMessage_Assignment {
	return &internal_v1.ClusterMessage_Assignment{
		Worker: model.UnwrapInstance(c.Worker),
		Grants: slicex.Map(c.Grants, UnwrapGrant),
	}
}

type Coordinator struct {
	instance model.Instance
	grant    Grant
}

func (c Coordinator) String() string {
	return fmt.Sprintf("%v@%v", c.instance, c.grant)
}

// Cluster contains information about location of coordinators responsible for services. Immutable.
type Cluster struct {
	services map[model.QualifiedServiceName]Coordinator
}

func NewCluster(assignments []Assignment) Cluster {
	services := make(map[model.QualifiedServiceName]Coordinator)
	for _, assignment := range assignments {
		for _, grant := range assignment.Grants {
			services[grant.Service()] = Coordinator{
				instance: assignment.Worker,
				grant:    grant,
			}
		}
	}
	return Cluster{services: services}
}

func (c Cluster) Update(assignments []Assignment) {
	for _, assignment := range assignments {
		for _, grant := range assignment.Grants {
			c.services[grant.Service()] = Coordinator{
				instance: assignment.Worker,
				grant:    grant,
			}
		}
	}
}

func (c Cluster) Remove(services []model.QualifiedServiceName) {
	for _, service := range services {
		delete(c.services, service)
	}
}

func (c Cluster) Service(service model.QualifiedServiceName) (Coordinator, bool) {
	coordinator, ok := c.services[service]
	return coordinator, ok
}

func (c Cluster) Services() map[model.QualifiedServiceName]Coordinator {
	return mapx.Clone(c.services)
}

func (c Cluster) String() string {
	return fmt.Sprintf("cluster{%v}", c.services)
}
