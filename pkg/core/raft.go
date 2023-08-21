package core

import (
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pb/private"
)

func NewRaftCommandNewTenant(tenant model.Tenant) *internal_v1.RaftCommand {
	return &internal_v1.RaftCommand{
		Update: &internal_v1.RaftCommand_Tenant{
			Tenant: &internal_v1.TenantCommand{
				New: &internal_v1.TenantCommand_New{
					Tenant: model.UnwrapTenant(tenant),
				},
			},
		},
	}
}

func NewRaftCommandUpdateTenant(tenant model.Tenant, guard model.Version) *internal_v1.RaftCommand {
	return &internal_v1.RaftCommand{
		Update: &internal_v1.RaftCommand_Tenant{
			Tenant: &internal_v1.TenantCommand{
				Update: &internal_v1.TenantCommand_Update{
					Tenant:  model.UnwrapTenant(tenant),
					Version: int64(guard),
				},
			},
		},
	}
}

func NewRaftCommandDeleteTenant(name model.TenantName) *internal_v1.RaftCommand {
	return &internal_v1.RaftCommand{
		Update: &internal_v1.RaftCommand_Tenant{
			Tenant: &internal_v1.TenantCommand{
				Delete: &internal_v1.TenantCommand_Delete{
					Name: string(name),
				},
			},
		},
	}
}

func NewRaftPlacementUpdate(upd *internal_v1.RaftCommand_Placement) *internal_v1.RaftCommand {
	return &internal_v1.RaftCommand{
		Update: upd,
	}
}
