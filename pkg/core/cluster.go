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

// Cluster contains information about location of coordinators responsible for tenants. Immutable.
type Cluster struct {
	tenants map[model.TenantName]Coordinator
}

func (c Cluster) Tenant(tenant model.TenantName) (Coordinator, bool) {
	coordinator, ok := c.tenants[tenant]
	return coordinator, ok
}

func (c Cluster) Tenants() map[model.TenantName]Coordinator {
	return mapx.Clone(c.tenants)
}

func (c Cluster) String() string {
	return fmt.Sprintf("cluster{%v}", c.tenants)
}
