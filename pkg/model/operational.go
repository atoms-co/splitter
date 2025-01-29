package model

import (
	"go.atoms.co/slicex"
	"go.atoms.co/splitter/pb"
	"github.com/golang/protobuf/proto"
)

type TenantOperationalOption func(*public_v1.Tenant_Operational)

func WithTenantOperationalBannedRegions(regions ...Region) TenantOperationalOption {
	return func(t *public_v1.Tenant_Operational) {
		t.BannedRegions = slicex.Map(regions, func(r Region) string {
			return string(r)
		})
	}
}

type TenantOperational struct {
	pb *public_v1.Tenant_Operational
}

func NewTenantOperational(opts ...TenantOperationalOption) TenantOperational {
	pb := &public_v1.Tenant_Operational{}
	for _, fn := range opts {
		fn(pb)
	}
	return WrapTenantOperational(pb)
}

func UpdateTenantOperational(tenant Tenant, opts ...TenantOperationalOption) (TenantOperational, error) {
	pb := UnwrapTenant(tenant).Operational
	if pb == nil {
		pb = &public_v1.Tenant_Operational{}
	}
	pb = proto.Clone(pb).(*public_v1.Tenant_Operational)
	for _, fn := range opts {
		fn(pb)
	}
	if _, err := UpdateTenant(tenant, WithTenantOperational(WrapTenantOperational(pb))); err != nil {
		return TenantOperational{}, err
	}
	return WrapTenantOperational(pb), nil
}

func WrapTenantOperational(pb *public_v1.Tenant_Operational) TenantOperational {
	return TenantOperational{pb: pb}
}

func UnwrapTenantOperational(t TenantOperational) *public_v1.Tenant_Operational {
	return t.pb
}

func (t TenantOperational) BannedRegions() []Region {
	return slicex.Map(t.pb.GetBannedRegions(), func(r string) Region {
		return Region(r)
	})
}

func (t TenantOperational) String() string {
	return proto.MarshalTextString(t.pb)
}

type ServiceOperationalOption func(*public_v1.Service_Operational)

func WithServiceOperationalLocked(locked bool) ServiceOperationalOption {
	return func(s *public_v1.Service_Operational) {
		s.Locked = locked
	}
}

func WithServiceOperationalBannedRegions(regions ...Region) ServiceOperationalOption {
	return func(t *public_v1.Service_Operational) {
		t.BannedRegions = slicex.Map(regions, func(r Region) string {
			return string(r)
		})
	}
}

func WithServiceOperationalDisableLoadBalance(disable bool) ServiceOperationalOption {
	return func(t *public_v1.Service_Operational) {
		t.DisableLoadBalance = disable
	}
}

type ServiceOperational struct {
	pb *public_v1.Service_Operational
}

func NewServiceOperational(opts ...ServiceOperationalOption) ServiceOperational {
	pb := &public_v1.Service_Operational{}
	for _, fn := range opts {
		fn(pb)
	}
	return WrapServiceOperational(pb)
}

func UpdateServiceOperational(service Service, opts ...ServiceOperationalOption) (ServiceOperational, error) {
	pb := UnwrapService(service).Operational
	if pb == nil {
		pb = &public_v1.Service_Operational{}
	}
	pb = proto.Clone(pb).(*public_v1.Service_Operational)
	for _, fn := range opts {
		fn(pb)
	}
	if _, err := UpdateService(service, WithServiceOperational(WrapServiceOperational(pb))); err != nil {
		return ServiceOperational{}, err
	}
	return WrapServiceOperational(pb), nil
}

func WrapServiceOperational(pb *public_v1.Service_Operational) ServiceOperational {
	return ServiceOperational{pb: pb}
}

func UnwrapServiceOperational(t ServiceOperational) *public_v1.Service_Operational {
	return t.pb
}

func (t ServiceOperational) BannedRegions() []Region {
	return slicex.Map(t.pb.GetBannedRegions(), func(r string) Region {
		return Region(r)
	})
}

func (t ServiceOperational) DisableLoadBalance() bool {
	return t.pb.GetDisableLoadBalance()
}

func (t ServiceOperational) String() string {
	return proto.MarshalTextString(t.pb)
}

type DomainOperationalOption func(*public_v1.Domain_Operational)

func WithDomainOperationalLocked(locked bool) DomainOperationalOption {
	return func(d *public_v1.Domain_Operational) {
		d.Locked = locked
	}
}

func WithDomainOperationalBannedRegions(regions ...Region) DomainOperationalOption {
	return func(t *public_v1.Domain_Operational) {
		t.BannedRegions = slicex.Map(regions, func(r Region) string {
			return string(r)
		})
	}
}

type DomainOperational struct {
	pb *public_v1.Domain_Operational
}

func NewDomainOperational(opts ...DomainOperationalOption) DomainOperational {
	pb := &public_v1.Domain_Operational{}
	for _, fn := range opts {
		fn(pb)
	}
	return WrapDomainOperational(pb)
}

func UpdateDomainOperational(domain Domain, opts ...DomainOperationalOption) (DomainOperational, error) {
	pb := UnwrapDomain(domain).Operational
	if pb == nil {
		pb = &public_v1.Domain_Operational{}
	}
	pb = proto.Clone(pb).(*public_v1.Domain_Operational)
	for _, fn := range opts {
		fn(pb)
	}
	if _, err := UpdateDomain(domain, WithDomainOperational(WrapDomainOperational(pb))); err != nil {
		return DomainOperational{}, err
	}
	return WrapDomainOperational(pb), nil
}

func WrapDomainOperational(pb *public_v1.Domain_Operational) DomainOperational {
	return DomainOperational{pb: pb}
}

func UnwrapDomainOperational(t DomainOperational) *public_v1.Domain_Operational {
	return t.pb
}

func (t DomainOperational) BannedRegions() []Region {
	return slicex.Map(t.pb.GetBannedRegions(), func(r string) Region {
		return Region(r)
	})
}

func (t DomainOperational) String() string {
	return proto.MarshalTextString(t.pb)
}

func (t DomainOperational) Locked() bool { return t.pb.GetLocked() }
