package core

import (
	"go.atoms.co/lib/encoding/protox"
	"go.atoms.co/slicex"
	splitterpb "go.atoms.co/splitter/pb"
	splitterprivatepb "go.atoms.co/splitter/pb/private"
	"go.atoms.co/splitter/pkg/model"
)

// Snapshot holds the complete state of all tenants.
type Snapshot struct {
	pb *splitterprivatepb.Snapshot
}

func NewSnapshot(tenants ...State) Snapshot {
	return Snapshot{pb: &splitterprivatepb.Snapshot{
		Tenants: slicex.Map(tenants, UnwrapState),
	}}
}

func WrapSnapshot(pb *splitterprivatepb.Snapshot) Snapshot {
	return Snapshot{pb: pb}
}

func UnwrapSnapshot(t Snapshot) *splitterprivatepb.Snapshot {
	return t.pb
}

func (s Snapshot) Tenants() []State {
	return slicex.Map(s.pb.GetTenants(), WrapState)
}

func (s Snapshot) String() string {
	return protox.MarshalTextString(s.pb)
}

func (s Snapshot) Size() int {
	return protox.Size(s.pb)
}

// State is the complete state of a single tenant.
type State struct {
	pb *splitterprivatepb.State
}

func NewState(tenant model.TenantInfo, services []model.ServiceInfoEx, placements []InternalPlacementInfo, statuses []ServiceStatus) State {
	return State{pb: &splitterprivatepb.State{
		Tenant:     model.UnwrapTenantInfo(tenant),
		Services:   slicex.Map(services, model.UnwrapServiceInfoEx),
		Placements: slicex.Map(placements, UnwrapInternalPlacementInfo),
		Statuses:   slicex.Map(statuses, UnwrapServiceStatus),
	}}
}

func WrapState(pb *splitterprivatepb.State) State {
	return State{pb: pb}
}

func UnwrapState(t State) *splitterprivatepb.State {
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

func (s State) Statuses() []ServiceStatus {
	return slicex.Map(s.pb.GetStatuses(), WrapServiceStatus)
}

func (s State) Size() int {
	return protox.Size(s.pb)
}

func (s State) String() string {
	return protox.MarshalTextString(s.pb)
}

// Update is an incremental update to a single tenant. Updates to multiple aspects can be made
// atomically. New entities are identified with v1. Should not be empty.
type Update struct {
	pb *splitterprivatepb.Update
}

func NewTenantUpdate(t model.TenantInfo) Update {
	return WrapUpdate(&splitterprivatepb.Update{
		Name: string(t.Name()),
		Tenant: &splitterprivatepb.Update_Tenant{
			Updated: model.UnwrapTenantInfo(t),
		},
	})
}

func NewServiceUpdate(s model.ServiceInfo) Update {
	return WrapUpdate(&splitterprivatepb.Update{
		Name: string(s.Name().Tenant),
		Service: &splitterprivatepb.Update_Service{
			Updated: []*splitterprivatepb.ServiceUpdate{{Service: model.UnwrapServiceInfo(s)}},
		},
	})
}

func NewServiceRemoval(s model.QualifiedServiceName) Update {
	return WrapUpdate(&splitterprivatepb.Update{
		Name: string(s.Tenant),
		Service: &splitterprivatepb.Update_Service{
			Removed: []*splitterpb.QualifiedServiceName{s.ToProto()},
		},
	})
}

func NewDomainUpdate(s model.ServiceInfo, d model.Domain) Update {
	return WrapUpdate(&splitterprivatepb.Update{
		Name: string(s.Name().Tenant),
		Service: &splitterprivatepb.Update_Service{
			Updated: []*splitterprivatepb.ServiceUpdate{
				{
					Service: model.UnwrapServiceInfo(s),
					Updated: []*splitterpb.Domain{model.UnwrapDomain(d)},
				},
			},
		},
	})
}

func NewDomainRemoval(s model.ServiceInfo, d model.QualifiedDomainName) Update {
	return WrapUpdate(&splitterprivatepb.Update{
		Name: string(s.Name().Tenant),
		Service: &splitterprivatepb.Update_Service{
			Updated: []*splitterprivatepb.ServiceUpdate{
				{
					Service: model.UnwrapServiceInfo(s),
					Removed: []*splitterpb.QualifiedDomainName{d.ToProto()},
				},
			},
		},
	})
}

func NewPlacementUpdate(t InternalPlacementInfo) Update {
	return WrapUpdate(&splitterprivatepb.Update{
		Name: string(t.Name().Tenant),
		Placement: &splitterprivatepb.Update_Placement{
			Updated: []*splitterprivatepb.InternalPlacementInfo{UnwrapInternalPlacementInfo(t)},
		},
	})
}

func NewPlacementRemoval(t model.QualifiedPlacementName) Update {
	return WrapUpdate(&splitterprivatepb.Update{
		Name: string(t.Tenant),
		Placement: &splitterprivatepb.Update_Placement{
			Removed: []*splitterpb.QualifiedPlacementName{t.ToProto()},
		},
	})
}

func NewServiceStatusUpdate(s ServiceStatus) Update {
	return WrapUpdate(&splitterprivatepb.Update{
		Name:   string(s.Load().Service().Tenant),
		Status: UnwrapServiceStatus(s),
	})
}

func WrapUpdate(pb *splitterprivatepb.Update) Update {
	return Update{pb: pb}
}

func UnwrapUpdate(t Update) *splitterprivatepb.Update {
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
	return slicex.Map(s.pb.GetService().GetRemoved(), func(t *splitterpb.QualifiedServiceName) model.QualifiedServiceName {
		ret, _ := model.ParseQualifiedServiceName(t)
		return ret
	})
}

func (s Update) ServiceStatus() (ServiceStatus, bool) {
	status := s.pb.GetStatus()
	if status == nil {
		return ServiceStatus{}, false
	}

	return WrapServiceStatus(status), true
}

// IsStateUpdated indicates if Update contains any State update, including Tenant, Service and Placements
func (s Update) IsStateUpdated() bool {
	if _, ok := s.TenantUpdated(); ok {
		return true
	}

	return len(s.ServicesUpdated()) > 0 || len(s.ServicesRemoved()) > 0 || len(s.PlacementsUpdated()) > 0 || len(s.PlacementsRemoved()) > 0
}

// ServiceUpdate is an incremental update to a single service.
type ServiceUpdate struct {
	pb *splitterprivatepb.ServiceUpdate
}

func WrapServiceUpdate(pb *splitterprivatepb.ServiceUpdate) ServiceUpdate {
	return ServiceUpdate{pb: pb}
}

func UnwrapServiceUpdate(t ServiceUpdate) *splitterprivatepb.ServiceUpdate {
	return t.pb
}
func (s ServiceUpdate) Service() model.ServiceInfo {
	return model.WrapServiceInfo(s.pb.GetService())
}
func (s ServiceUpdate) DomainsUpdated() []model.Domain {
	return slicex.Map(s.pb.GetUpdated(), model.WrapDomain)
}

func (s ServiceUpdate) DomainsRemoved() []model.QualifiedDomainName {
	return slicex.Map(s.pb.GetRemoved(), func(t *splitterpb.QualifiedDomainName) model.QualifiedDomainName {
		ret, _ := model.ParseQualifiedDomainName(t)
		return ret
	})
}

func (s Update) PlacementsUpdated() []InternalPlacementInfo {
	return slicex.Map(s.pb.GetPlacement().GetUpdated(), WrapInternalPlacementInfo)
}

func (s Update) PlacementsRemoved() []model.QualifiedPlacementName {
	return slicex.Map(s.pb.GetPlacement().GetRemoved(), func(t *splitterpb.QualifiedPlacementName) model.QualifiedPlacementName {
		ret, _ := model.ParseQualifiedPlacementName(t)
		return ret
	})
}

func (s Update) Size() int {
	return protox.Size(s.pb)
}

func (s Update) String() string {
	return protox.MarshalTextString(s.pb)
}

// Delete removes an entire tenant.
type Delete struct {
	pb *splitterprivatepb.Delete
}

func NewDelete(name model.TenantName) Delete {
	return WrapDelete(&splitterprivatepb.Delete{
		Tenant: string(name),
	})
}
func WrapDelete(pb *splitterprivatepb.Delete) Delete {
	return Delete{pb: pb}
}

func UnwrapDelete(t Delete) *splitterprivatepb.Delete {
	return t.pb
}

func (s Delete) Tenant() model.TenantName {
	return model.TenantName(s.pb.GetTenant())
}

func (s Delete) Size() int {
	return protox.Size(s.pb)
}

func (s Delete) String() string {
	return protox.MarshalTextString(s.pb)
}

// Restore restores the FSM to a snapshot.
type Restore struct {
	pb *splitterprivatepb.Restore
}

func NewRestore(snapshot Snapshot) Restore {
	return WrapRestore(&splitterprivatepb.Restore{
		Snapshot: UnwrapSnapshot(snapshot),
	})
}
func WrapRestore(pb *splitterprivatepb.Restore) Restore {
	return Restore{pb: pb}
}

func UnwrapRestore(t Restore) *splitterprivatepb.Restore {
	return t.pb
}

func (s Restore) Snapshot() Snapshot {
	return WrapSnapshot(s.pb.GetSnapshot())
}

func (s Restore) Size() int {
	return protox.Size(s.pb)
}

type ServiceStatus struct {
	pb *splitterprivatepb.ServiceStatus
}

func NewServiceStatus(serviceLoad ServiceLoadInfo) ServiceStatus {
	return ServiceStatus{pb: &splitterprivatepb.ServiceStatus{
		Load: UnwrapServiceLoadInfo(serviceLoad),
	}}
}

func WrapServiceStatus(pb *splitterprivatepb.ServiceStatus) ServiceStatus {
	return ServiceStatus{pb: pb}
}

func UnwrapServiceStatus(m ServiceStatus) *splitterprivatepb.ServiceStatus {
	return m.pb
}

func (m ServiceStatus) Load() ServiceLoadInfo {
	return WrapServiceLoadInfo(m.pb.GetLoad())
}

func (m ServiceStatus) String() string {
	return protox.MarshalTextString(m.pb)
}

func (m ServiceStatus) Equal(other ServiceStatus) bool {
	return protox.Equal(m.pb, other.pb)
}
