package storage

import (
	"go.atoms.co/slicex"
	"go.atoms.co/splitter/pkg/core"
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

func NewState(tenant model.TenantInfo, domains []model.DomainInfo, placements []core.InternalPlacementInfo) State {
	return State{pb: &internal_v1.State{
		Tenant:     model.UnwrapTenantInfo(tenant),
		Domains:    slicex.Map(domains, model.UnwrapDomainInfo),
		Placements: slicex.Map(placements, core.UnwrapInternalPlacementInfo),
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

func (s State) Domains() []model.DomainInfo {
	return slicex.Map(s.pb.GetDomains(), model.WrapDomainInfo)
}

func (s State) Placements() []core.InternalPlacementInfo {
	return slicex.Map(s.pb.GetPlacements(), core.WrapInternalPlacementInfo)
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

func NewDomainUpdate(t model.DomainInfo) Update {
	return WrapUpdate(&internal_v1.Update{
		Name: string(t.Name().Service.Tenant),
		Domain: &internal_v1.Update_Domain{
			Updated: []*public_v1.DomainInfo{model.UnwrapDomainInfo(t)},
		},
	})
}

func NewDomainRemoval(t model.QualifiedDomainName) Update {
	return WrapUpdate(&internal_v1.Update{
		Name: string(t.Service.Tenant),
		Domain: &internal_v1.Update_Domain{
			Removed: []*public_v1.QualifiedDomainName{t.ToProto()},
		},
	})
}

func NewPlacementUpdate(t core.InternalPlacementInfo) Update {
	return WrapUpdate(&internal_v1.Update{
		Name: string(t.Name().Tenant),
		Placement: &internal_v1.Update_Placement{
			Updated: []*internal_v1.InternalPlacementInfo{core.UnwrapInternalPlacementInfo(t)},
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

func (s Update) DomainsUpdated() []model.DomainInfo {
	return slicex.Map(s.pb.GetDomain().GetUpdated(), model.WrapDomainInfo)
}

func (s Update) DomainsRemoved() []model.QualifiedDomainName {
	return slicex.Map(s.pb.GetDomain().GetRemoved(), func(t *public_v1.QualifiedDomainName) model.QualifiedDomainName {
		ret, _ := model.ParseQualifiedDomainName(t)
		return ret
	})
}

func (s Update) PlacementsUpdated() []core.InternalPlacementInfo {
	return slicex.Map(s.pb.GetPlacement().GetUpdated(), core.WrapInternalPlacementInfo)
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
