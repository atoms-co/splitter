package model

import (
	"go.atoms.co/splitter/pb"
	"fmt"
	"github.com/golang/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"time"
)

type TenantOption func(tenant *public_v1.Tenant)

func WithTenantConfig(cfg TenantConfig) TenantOption {
	return func(tenant *public_v1.Tenant) {
		tenant.Config = UnwrapTenantConfig(cfg)
	}
}

type TenantName string

// Tenant represents a top-level namespace.
type Tenant struct {
	pb *public_v1.Tenant
}

func NewTenant(name TenantName, now time.Time, opts ...TenantOption) (Tenant, error) {
	pb := &public_v1.Tenant{
		Name:    string(name),
		Config:  &public_v1.Tenant_Config{},
		Created: timestamppb.New(now),
	}
	for _, fn := range opts {
		fn(pb)
	}
	return ParseTenant(pb)
}

func ParseTenant(pb *public_v1.Tenant) (Tenant, error) {
	if err := validateTenant(pb); err != nil {
		return Tenant{}, fmt.Errorf("invalid tenant: %w", err)
	}
	return Tenant{pb: proto.Clone(pb).(*public_v1.Tenant)}, nil
}

func validateTenant(pb *public_v1.Tenant) error {
	return nil // TODO(jhhurwitz): 08/18/2023 Actually validate
}

func UpdateTenant(tenant Tenant, opts ...TenantOption) (Tenant, error) {
	upd := proto.Clone(tenant.pb).(*public_v1.Tenant)
	for _, fn := range opts {
		fn(upd)
	}
	return ParseTenant(upd)
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

func (t Tenant) Config() TenantConfig {
	return WrapTenantConfig(t.pb.GetConfig())
}

func (t Tenant) Equals(t1 Tenant) bool {
	return proto.Equal(t.pb, t1.pb)
}

func (t Tenant) String() string {
	return proto.MarshalTextString(t.pb)
}

type TenantConfigOption func(cfg *public_v1.Tenant_Config)

// TenantConfig holds tenant configuration.
type TenantConfig struct {
	pb *public_v1.Tenant_Config
}

func NewTenantConfig(opts ...TenantConfigOption) TenantConfig {
	pb := &public_v1.Tenant_Config{}
	for _, fn := range opts {
		fn(pb)
	}
	return WrapTenantConfig(pb)
}

func UpdateTenantConfig(tenant Tenant, opts ...TenantConfigOption) (TenantConfig, error) {
	pb := UnwrapTenant(tenant).Config
	if pb == nil {
		pb = &public_v1.Tenant_Config{}
	}
	pb = proto.Clone(pb).(*public_v1.Tenant_Config)
	for _, fn := range opts {
		fn(pb)
	}
	if _, err := UpdateTenant(tenant, WithTenantConfig(WrapTenantConfig(pb))); err != nil {
		return TenantConfig{}, err
	}
	return WrapTenantConfig(pb), nil
}

func WrapTenantConfig(pb *public_v1.Tenant_Config) TenantConfig {
	return TenantConfig{pb: pb}
}

func UnwrapTenantConfig(cfg TenantConfig) *public_v1.Tenant_Config {
	return cfg.pb
}

// TenantInfo captures the full tenant information.
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
	return t.Tenant().Name()
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
