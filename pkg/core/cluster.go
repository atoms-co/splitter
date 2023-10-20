package core

import (
	"go.atoms.co/lib/mapx"
	"go.atoms.co/splitter/pkg/model"
	"fmt"
)

// Cluster contains information about location of coordinators responsible for tenants. Immutable.
type Cluster struct {
	tenants map[model.TenantName]model.Instance
}

func NewCluster(tenants map[model.TenantName]model.Instance) Cluster {
	return Cluster{
		tenants: mapx.Clone(tenants),
	}
}

func (c Cluster) Tenant(tenant model.TenantName) (model.Instance, bool) {
	instance, ok := c.tenants[tenant]
	return instance, ok
}

func (c Cluster) Tenants() map[model.TenantName]model.Instance {
	return mapx.Clone(c.tenants)
}

func (c Cluster) String() string {
	return fmt.Sprintf("cluster{%v}", c.tenants)
}
