package core

import (
	"go.atoms.co/slicex"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pb/private"
	"go.atoms.co/splitter/pb"
	"github.com/golang/protobuf/proto"
)

// Snapshot holds the complete state of all tenants.
type Snapshot struct {
	pb *internal_v1.Snapshot
}

func NewSnapshot(tenants ...State) Snapshot {
	return Snapshot{pb: &internal_v1.Snapshot{
		Tenants: slicex.Map(tenants, UnwrapState),
	}}
}

func WrapSnapshot(pb *internal_v1.Snapshot) Snapshot {
	return Snapshot{pb: pb}
}

func UnwrapSnapshot(t Snapshot) *internal_v1.Snapshot {
	return t.pb
}

func (s Snapshot) Tenants() []State {
	return slicex.Map(s.pb.GetTenants(), WrapState)
}

func (s Snapshot) String() string {
	return proto.MarshalTextString(s.pb)
}

// State is the complete state of a single tenant.
type State struct {
	pb *internal_v1.State
}

func NewState(tenant model.TenantInfo, services []model.ServiceInfoEx, placements []InternalPlacementInfo) State {
	return State{pb: &internal_v1.State{
		Tenant:     model.UnwrapTenantInfo(tenant),
		Services:   slicex.Map(services, model.UnwrapServiceInfoEx),
		Placements: slicex.Map(placements, UnwrapInternalPlacementInfo),
	}}
}

func WrapState(pb *internal_v1.State) State {
	return State{pb: pb}
}

func UnwrapState(t State) *internal_v1.State {
	return t.pb
}

func (s State) Tenant() model.TenantInfo {
	return model.WrapTenantInfo(s.pb.GetTenant())
}

func (s State) Services() []model.ServiceInfoEx {
	return slicex.Map(s.pb.GetServices(), model.WrapServiceInfoEx)
}

func (s State) Placements() []InternalPlacementInfo {
	return slicex.Map(s.pb.GetPlacements(), WrapInternalPlacementInfo)
}

func (s State) String() string {
	return proto.MarshalTextString(s.pb)
}

// Update is an incremental update to a single tenant. Updates to multiple aspects can be made
// atomically. New entities are identified with v1. Should not be empty.
type Update struct {
	pb *internal_v1.Update
}

func NewTenantUpdate(t model.TenantInfo) Update {
	return WrapUpdate(&internal_v1.Update{
		Name: string(t.Name()),
		Tenant: &internal_v1.Update_Tenant{
			Updated: model.UnwrapTenantInfo(t),
		},
	})
}

func NewServiceUpdate(s model.ServiceInfo) Update {
	return WrapUpdate(&internal_v1.Update{
		Name: string(s.Name().Tenant),
		Service: &internal_v1.Update_Service{
			Updated: []*internal_v1.ServiceUpdate{{Service: model.UnwrapServiceInfo(s)}},
		},
	})
}

func NewServiceRemoval(s model.QualifiedServiceName) Update {
	return WrapUpdate(&internal_v1.Update{
		Name: string(s.Tenant),
		Service: &internal_v1.Update_Service{
			Removed: []*public_v1.QualifiedServiceName{s.ToProto()},
		},
	})
}

func NewDomainUpdate(s model.ServiceInfo, d model.Domain) Update {
	return WrapUpdate(&internal_v1.Update{
		Name: string(s.Name().Tenant),
		Service: &internal_v1.Update_Service{
			Updated: []*internal_v1.ServiceUpdate{
				{
					Service: model.UnwrapServiceInfo(s),
					Updated: []*public_v1.Domain{model.UnwrapDomain(d)},
				},
			},
		},
	})
}

func NewDomainRemoval(s model.ServiceInfo, d model.QualifiedDomainName) Update {
	return WrapUpdate(&internal_v1.Update{
		Name: string(s.Name().Tenant),
		Service: &internal_v1.Update_Service{
			Updated: []*internal_v1.ServiceUpdate{
				{
					Service: model.UnwrapServiceInfo(s),
					Removed: []*public_v1.QualifiedDomainName{d.ToProto()},
				},
			},
		},
	})
}

func NewPlacementUpdate(t InternalPlacementInfo) Update {
	return WrapUpdate(&internal_v1.Update{
		Name: string(t.Name().Tenant),
		Placement: &internal_v1.Update_Placement{
			Updated: []*internal_v1.InternalPlacementInfo{UnwrapInternalPlacementInfo(t)},
		},
	})
}

func NewPlacementRemoval(t model.QualifiedPlacementName) Update {
	return WrapUpdate(&internal_v1.Update{
		Name: string(t.Tenant),
		Placement: &internal_v1.Update_Placement{
			Removed: []*public_v1.QualifiedPlacementName{t.ToProto()},
		},
	})
}

func WrapUpdate(pb *internal_v1.Update) Update {
	return Update{pb: pb}
}

func UnwrapUpdate(t Update) *internal_v1.Update {
	return t.pb
}

func (s Update) Name() model.TenantName {
	return model.TenantName(s.pb.GetName())
}

func (s Update) TenantUpdated() (model.TenantInfo, bool) {
	if pb := s.pb.GetTenant().GetUpdated(); pb != nil {
		return model.WrapTenantInfo(pb), true
	}
	return model.TenantInfo{}, false
}

func (s Update) ServicesUpdated() []ServiceUpdate {
	return slicex.Map(s.pb.GetService().GetUpdated(), WrapServiceUpdate)
}

func (s Update) ServicesRemoved() []model.QualifiedServiceName {
	return slicex.Map(s.pb.GetService().GetRemoved(), func(t *public_v1.QualifiedServiceName) model.QualifiedServiceName {
		ret, _ := model.ParseQualifiedServiceName(t)
		return ret
	})
}

// ServiceUpdate is an incremental update to a single service.
type ServiceUpdate struct {
	pb *internal_v1.ServiceUpdate
}

func WrapServiceUpdate(pb *internal_v1.ServiceUpdate) ServiceUpdate {
	return ServiceUpdate{pb: pb}
}

func UnwrapServiceUpdate(t ServiceUpdate) *internal_v1.ServiceUpdate {
	return t.pb
}
func (s ServiceUpdate) Service() model.ServiceInfo {
	return model.WrapServiceInfo(s.pb.GetService())
}
func (s ServiceUpdate) DomainsUpdated() []model.Domain {
	return slicex.Map(s.pb.GetUpdated(), model.WrapDomain)
}

func (s ServiceUpdate) DomainsRemoved() []model.QualifiedDomainName {
	return slicex.Map(s.pb.GetRemoved(), func(t *public_v1.QualifiedDomainName) model.QualifiedDomainName {
		ret, _ := model.ParseQualifiedDomainName(t)
		return ret
	})
}

func (s Update) PlacementsUpdated() []InternalPlacementInfo {
	return slicex.Map(s.pb.GetPlacement().GetUpdated(), WrapInternalPlacementInfo)
}

func (s Update) PlacementsRemoved() []model.QualifiedPlacementName {
	return slicex.Map(s.pb.GetPlacement().GetRemoved(), func(t *public_v1.QualifiedPlacementName) model.QualifiedPlacementName {
		ret, _ := model.ParseQualifiedPlacementName(t)
		return ret
	})
}

func (s Update) String() string {
	return proto.MarshalTextString(s.pb)
}

// Delete removes an entire tenant.
type Delete struct {
	pb *internal_v1.Delete
}

func NewDelete(name model.TenantName) Delete {
	return WrapDelete(&internal_v1.Delete{
		Tenant: string(name),
	})
}
func WrapDelete(pb *internal_v1.Delete) Delete {
	return Delete{pb: pb}
}

func UnwrapDelete(t Delete) *internal_v1.Delete {
	return t.pb
}

func (s Delete) Tenant() model.TenantName {
	return model.TenantName(s.pb.GetTenant())
}

func (s Delete) String() string {
	return proto.MarshalTextString(s.pb)
}

// Restore restores the FSM to a snapshot.
type Restore struct {
	pb *internal_v1.Restore
}

func NewRestore(snapshot Snapshot) Restore {
	return WrapRestore(&internal_v1.Restore{
		Snapshot: UnwrapSnapshot(snapshot),
	})
}
func WrapRestore(pb *internal_v1.Restore) Restore {
	return Restore{pb: pb}
}

func UnwrapRestore(t Restore) *internal_v1.Restore {
	return t.pb
}

func (s Restore) Snapshot() Snapshot {
	return WrapSnapshot(s.pb.GetSnapshot())
}
