package model

import (
	"go.atoms.co/lib/encoding/protox"
	"go.atoms.co/slicex"
	splitterpb "go.atoms.co/splitter/pb"
)

type TenantOperationalOption func(*splitterpb.Tenant_Operational)

func WithTenantOperationalBannedRegions(regions ...Region) TenantOperationalOption {
	return func(t *splitterpb.Tenant_Operational) {
		t.BannedRegions = slicex.Map(regions, func(r Region) string {
			return string(r)
		})
	}
}

type TenantOperational struct {
	pb *splitterpb.Tenant_Operational
}

func NewTenantOperational(opts ...TenantOperationalOption) TenantOperational {
	pb := &splitterpb.Tenant_Operational{}
	for _, fn := range opts {
		fn(pb)
	}
	return WrapTenantOperational(pb)
}

func UpdateTenantOperational(tenant Tenant, opts ...TenantOperationalOption) (TenantOperational, error) {
	pb := UnwrapTenant(tenant).Operational
	if pb == nil {
		pb = &splitterpb.Tenant_Operational{}
	}
	pb = protox.Clone(pb)
	for _, fn := range opts {
		fn(pb)
	}
	if _, err := UpdateTenant(tenant, WithTenantOperational(WrapTenantOperational(pb))); err != nil {
		return TenantOperational{}, err
	}
	return WrapTenantOperational(pb), nil
}

func WrapTenantOperational(pb *splitterpb.Tenant_Operational) TenantOperational {
	return TenantOperational{pb: pb}
}

func UnwrapTenantOperational(t TenantOperational) *splitterpb.Tenant_Operational {
	return t.pb
}

func (t TenantOperational) BannedRegions() []Region {
	return slicex.Map(t.pb.GetBannedRegions(), func(r string) Region {
		return Region(r)
	})
}

func (t TenantOperational) String() string {
	return protox.MarshalTextString(t.pb)
}

type ServiceOperationalOption func(*splitterpb.Service_Operational)

func WithServiceOperationalLocked(locked bool) ServiceOperationalOption {
	return func(s *splitterpb.Service_Operational) {
		s.Locked = locked
	}
}

func WithServiceOperationalBannedRegions(regions ...Region) ServiceOperationalOption {
	return func(t *splitterpb.Service_Operational) {
		t.BannedRegions = slicex.Map(regions, func(r Region) string {
			return string(r)
		})
	}
}

func WithServiceOperationalDisableLoadBalance(disable bool) ServiceOperationalOption {
	return func(t *splitterpb.Service_Operational) {
		t.DisableLoadBalance = disable
	}
}

func WithServiceOperationalVerboseLogging(verbose bool) ServiceOperationalOption {
	return func(t *splitterpb.Service_Operational) {
		t.VerboseLogging = verbose
	}
}

type ServiceOperational struct {
	pb *splitterpb.Service_Operational
}

func NewServiceOperational(opts ...ServiceOperationalOption) ServiceOperational {
	pb := &splitterpb.Service_Operational{}
	for _, fn := range opts {
		fn(pb)
	}
	return WrapServiceOperational(pb)
}

func UpdateServiceOperational(service Service, opts ...ServiceOperationalOption) (ServiceOperational, error) {
	pb := UnwrapService(service).Operational
	if pb == nil {
		pb = &splitterpb.Service_Operational{}
	}
	pb = protox.Clone(pb)
	for _, fn := range opts {
		fn(pb)
	}
	if _, err := UpdateService(service, WithServiceOperational(WrapServiceOperational(pb))); err != nil {
		return ServiceOperational{}, err
	}
	return WrapServiceOperational(pb), nil
}

func WrapServiceOperational(pb *splitterpb.Service_Operational) ServiceOperational {
	return ServiceOperational{pb: pb}
}

func UnwrapServiceOperational(t ServiceOperational) *splitterpb.Service_Operational {
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

func (t ServiceOperational) VerboseLogging() bool {
	return t.pb.GetVerboseLogging()
}

func (t ServiceOperational) String() string {
	return protox.MarshalTextString(t.pb)
}

type DomainOperationalOption func(*splitterpb.Domain_Operational)

func WithDomainOperationalLocked(locked bool) DomainOperationalOption {
	return func(d *splitterpb.Domain_Operational) {
		d.Locked = locked
	}
}

func WithDomainOperationalBannedRegions(regions ...Region) DomainOperationalOption {
	return func(t *splitterpb.Domain_Operational) {
		t.BannedRegions = slicex.Map(regions, func(r Region) string {
			return string(r)
		})
	}
}

type DomainOperational struct {
	pb *splitterpb.Domain_Operational
}

func NewDomainOperational(opts ...DomainOperationalOption) DomainOperational {
	pb := &splitterpb.Domain_Operational{}
	for _, fn := range opts {
		fn(pb)
	}
	return WrapDomainOperational(pb)
}

func UpdateDomainOperational(domain Domain, opts ...DomainOperationalOption) (DomainOperational, error) {
	pb := UnwrapDomain(domain).Operational
	if pb == nil {
		pb = &splitterpb.Domain_Operational{}
	}
	pb = protox.Clone(pb)
	for _, fn := range opts {
		fn(pb)
	}
	if _, err := UpdateDomain(domain, WithDomainOperational(WrapDomainOperational(pb))); err != nil {
		return DomainOperational{}, err
	}
	return WrapDomainOperational(pb), nil
}

func WrapDomainOperational(pb *splitterpb.Domain_Operational) DomainOperational {
	return DomainOperational{pb: pb}
}

func UnwrapDomainOperational(t DomainOperational) *splitterpb.Domain_Operational {
	return t.pb
}

func (t DomainOperational) BannedRegions() []Region {
	return slicex.Map(t.pb.GetBannedRegions(), func(r string) Region {
		return Region(r)
	})
}

func (t DomainOperational) String() string {
	return protox.MarshalTextString(t.pb)
}

func (t DomainOperational) Locked() bool { return t.pb.GetLocked() }
