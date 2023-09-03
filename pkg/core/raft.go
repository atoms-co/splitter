package core

import (
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pb/private"
)

func NewRaftCommandNewTenant(tenant model.Tenant) *internal_v1.RaftCommand {
	return &internal_v1.RaftCommand{
		Command: &internal_v1.RaftCommand_Tenant{
			Tenant: &internal_v1.TenantRaftCommand{
				Tenant: &internal_v1.TenantRaftCommand_New_{
					New: &internal_v1.TenantRaftCommand_New{
						Tenant: model.UnwrapTenant(tenant),
					},
				},
			},
		},
	}
}

func NewRaftCommandUpdateTenant(tenant model.Tenant, guard model.Version) *internal_v1.RaftCommand {
	return &internal_v1.RaftCommand{
		Command: &internal_v1.RaftCommand_Tenant{
			Tenant: &internal_v1.TenantRaftCommand{
				Tenant: &internal_v1.TenantRaftCommand_Update_{
					Update: &internal_v1.TenantRaftCommand_Update{
						Tenant:  model.UnwrapTenant(tenant),
						Version: int64(guard),
					},
				},
			},
		},
	}
}

func NewRaftCommandDeleteTenant(name model.TenantName) *internal_v1.RaftCommand {
	return &internal_v1.RaftCommand{
		Command: &internal_v1.RaftCommand_Tenant{
			Tenant: &internal_v1.TenantRaftCommand{
				Tenant: &internal_v1.TenantRaftCommand_Delete_{
					Delete: &internal_v1.TenantRaftCommand_Delete{
						Name: string(name),
					},
				},
			},
		},
	}
}

func NewRaftCommandNewDomain(domain model.Domain) *internal_v1.RaftCommand {
	return &internal_v1.RaftCommand{
		Command: &internal_v1.RaftCommand_Domain{
			Domain: &internal_v1.DomainRaftCommand{
				Domain: &internal_v1.DomainRaftCommand_New_{
					New: &internal_v1.DomainRaftCommand_New{
						Domain: model.UnwrapDomain(domain),
					},
				},
			},
		},
	}
}

func NewRaftCommandUpdateDomain(domain model.Domain, guard model.Version) *internal_v1.RaftCommand {
	return &internal_v1.RaftCommand{
		Command: &internal_v1.RaftCommand_Domain{
			Domain: &internal_v1.DomainRaftCommand{
				Domain: &internal_v1.DomainRaftCommand_Update_{
					Update: &internal_v1.DomainRaftCommand_Update{
						Domain:  model.UnwrapDomain(domain),
						Version: int64(guard),
					},
				},
			},
		},
	}
}

func NewRaftCommandDeleteDomain(name model.QualifiedDomainName) *internal_v1.RaftCommand {
	return &internal_v1.RaftCommand{
		Command: &internal_v1.RaftCommand_Domain{
			Domain: &internal_v1.DomainRaftCommand{
				Domain: &internal_v1.DomainRaftCommand_Delete_{
					Delete: &internal_v1.DomainRaftCommand_Delete{
						Name: name.ToProto(),
					},
				},
			},
		},
	}
}

func NewRaftCommandNewPlacement(placement InternalPlacement) *internal_v1.RaftCommand {
	return &internal_v1.RaftCommand{
		Command: &internal_v1.RaftCommand_Placement{
			Placement: &internal_v1.PlacementRaftCommand{
				Placement: &internal_v1.PlacementRaftCommand_New_{
					New: &internal_v1.PlacementRaftCommand_New{
						Placement: UnwrapInternalPlacement(placement),
					},
				},
			},
		},
	}
}

func NewRaftCommandUpdatePlacement(placement InternalPlacement, guard model.Version) *internal_v1.RaftCommand {
	return &internal_v1.RaftCommand{
		Command: &internal_v1.RaftCommand_Placement{
			Placement: &internal_v1.PlacementRaftCommand{
				Placement: &internal_v1.PlacementRaftCommand_Update_{
					Update: &internal_v1.PlacementRaftCommand_Update{
						Placement: UnwrapInternalPlacement(placement),
						Version:   int64(guard),
					},
				},
			},
		},
	}
}

func NewRaftCommandDeletePlacement(name model.QualifiedPlacementName) *internal_v1.RaftCommand {
	return &internal_v1.RaftCommand{
		Command: &internal_v1.RaftCommand_Placement{
			Placement: &internal_v1.PlacementRaftCommand{
				Placement: &internal_v1.PlacementRaftCommand_Delete_{
					Delete: &internal_v1.PlacementRaftCommand_Delete{
						Name: name.ToProto(),
					},
				},
			},
		},
	}
}

func NewTenantRaftResponseUpdateTenant(tenant model.TenantInfo) *internal_v1.TenantRaftResponse {
	return &internal_v1.TenantRaftResponse{
		Tenant: &internal_v1.TenantRaftResponse_Update_{
			Update: &internal_v1.TenantRaftResponse_Update{
				Tenant: model.UnwrapTenantInfo(tenant),
			},
		},
	}
}

func NewDomainRaftResponseUpdateDomain(domain model.DomainInfo) *internal_v1.DomainRaftResponse {
	return &internal_v1.DomainRaftResponse{
		Domain: &internal_v1.DomainRaftResponse_Update_{
			Update: &internal_v1.DomainRaftResponse_Update{
				Domain: model.UnwrapDomainInfo(domain),
			},
		},
	}
}

func NewPlacementRaftResponseNewPlacement(placement InternalPlacementInfo) *internal_v1.PlacementRaftResponse {
	return &internal_v1.PlacementRaftResponse{
		Placement: &internal_v1.PlacementRaftResponse_New_{
			New: &internal_v1.PlacementRaftResponse_New{
				Placement: UnwrapInternalPlacementInfo(placement),
			},
		},
	}
}

func NewPlacementRaftResponseUpdatePlacement(placement InternalPlacementInfo) *internal_v1.PlacementRaftResponse {
	return &internal_v1.PlacementRaftResponse{
		Placement: &internal_v1.PlacementRaftResponse_Update_{
			Update: &internal_v1.PlacementRaftResponse_Update{
				Placement: UnwrapInternalPlacementInfo(placement),
			},
		},
	}
}
