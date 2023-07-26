package model

import (
	"go.atoms.co/splitter/pb"
	"github.com/golang/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"time"
)

type TenantOption func(tenant *public_v1.Tenant)

func WithTenantRegion(region Region) TenantOption {
	return func(tenant *public_v1.Tenant) {
		tenant.Config.Region = string(region)
	}
}

type TenantName string

// Tenant represents a top-level namespace.
type Tenant struct {
	pb *public_v1.Tenant
}

func NewTenant(name TenantName, now time.Time, opts ...TenantOption) Tenant {
	pb := &public_v1.Tenant{
		Name:    string(name),
		Config:  &public_v1.Tenant_Config{},
		Created: timestamppb.New(now),
	}
	for _, fn := range opts {
		fn(pb)
	}
	return WrapTenant(pb)
}

func UpdateTenant(tenant Tenant, opts ...TenantOption) Tenant {
	upd := proto.Clone(tenant.pb).(*public_v1.Tenant)
	for _, fn := range opts {
		fn(upd)
	}
	return WrapTenant(upd)
}

func WrapTenant(tenant *public_v1.Tenant) Tenant {
	return Tenant{pb: tenant}
}

func UnwrapTenant(tenant Tenant) *public_v1.Tenant {
	return tenant.pb
}

func (t Tenant) Name() TenantName {
	return TenantName(t.pb.GetName())
}

// Region returns a region preference for coordinator and UNIT/GLOBAL domains. Optional.
func (t Tenant) Region() (Region, bool) {
	r := t.pb.GetConfig().GetRegion()
	return Region(r), r != ""
}

func (t Tenant) Equals(t1 Tenant) bool {
	return proto.Equal(t.pb, t1.pb)
}

func (t Tenant) String() string {
	return proto.MarshalTextString(t.pb)
}

// TenantInfo captures the full topic information.
type TenantInfo struct {
	pb *public_v1.TenantInfo
}

func WrapTenantInfo(pb *public_v1.TenantInfo) TenantInfo {
	return TenantInfo{pb: pb}
}

func UnwrapTenantInfo(t TenantInfo) *public_v1.TenantInfo {
	return t.pb
}

func NewTenantInfo(tenant Tenant, version Version, now time.Time) TenantInfo {
	return WrapTenantInfo(&public_v1.TenantInfo{
		Tenant:    UnwrapTenant(tenant),
		Version:   int64(version),
		Timestamp: timestamppb.New(now),
	})
}

func (t TenantInfo) Name() TenantName {
	return TenantName(t.pb.GetTenant().GetName())
}

func (t TenantInfo) Tenant() Tenant {
	return WrapTenant(t.pb.GetTenant())
}

func (t TenantInfo) Version() Version {
	return Version(t.pb.GetVersion())
}

func (t TenantInfo) Timestamp() time.Time {
	return t.pb.GetTimestamp().AsTime()
}

func (t TenantInfo) String() string {
	return proto.MarshalTextString(t.pb)
}
