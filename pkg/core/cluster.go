package core

import (
	"go.atoms.co/lib/mapx"
	"go.atoms.co/splitter/pkg/model"
	"fmt"
)

type Coordinator struct {
	instance model.Instance
	gid      GrantID
}

// Cluster contains information about location of coordinators responsible for services. Immutable.
type Cluster struct {
	services map[model.QualifiedServiceName]Coordinator
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
