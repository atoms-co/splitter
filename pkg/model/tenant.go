package model

import (
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"go.atoms.co/lib/encoding/protox"
	splitterpb "go.atoms.co/splitter/pb"
)

type TenantName string

type TenantOption func(tenant *splitterpb.Tenant)

func WithTenantOperational(t TenantOperational) TenantOption {
	return func(tenant *splitterpb.Tenant) {
		tenant.Operational = UnwrapTenantOperational(t)
	}
}

func WithTenantConfig(cfg TenantConfig) TenantOption {
	return func(tenant *splitterpb.Tenant) {
		tenant.Config = UnwrapTenantConfig(cfg)
	}
}

// Tenant represents a top-level namespace.
type Tenant struct {
	pb *splitterpb.Tenant
}

func NewTenant(name TenantName, now time.Time, opts ...TenantOption) (Tenant, error) {
	pb := &splitterpb.Tenant{
		Name:    string(name),
		Config:  &splitterpb.Tenant_Config{},
		Created: timestamppb.New(now),
	}
	for _, fn := range opts {
		fn(pb)
	}
	return ParseTenant(pb)
}

func ParseTenant(pb *splitterpb.Tenant) (Tenant, error) {
	if err := validateTenant(pb); err != nil {
		return Tenant{}, fmt.Errorf("invalid tenant: %w", err)
	}
	return Tenant{pb: protox.Clone(pb)}, nil
}

func validateTenant(pb *splitterpb.Tenant) error {
	return nil // TODO(jhhurwitz): 08/18/2023 Actually validate
}

func UpdateTenant(tenant Tenant, opts ...TenantOption) (Tenant, error) {
	upd := protox.Clone(tenant.pb)
	for _, fn := range opts {
		fn(upd)
	}
	return ParseTenant(upd)
}

func WrapTenant(tenant *splitterpb.Tenant) Tenant {
	return Tenant{pb: tenant}
}

func UnwrapTenant(tenant Tenant) *splitterpb.Tenant {
	return tenant.pb
}

func (t Tenant) Name() TenantName {
	return TenantName(t.pb.GetName())
}

func (t Tenant) Operational() TenantOperational {
	return WrapTenantOperational(t.pb.GetOperational())
}

func (t Tenant) Config() TenantConfig {
	return WrapTenantConfig(t.pb.GetConfig())
}

func (t Tenant) Equals(t1 Tenant) bool {
	return protox.Equal(t.pb, t1.pb)
}

func (t Tenant) String() string {
	return protox.MarshalTextString(t.pb)
}

type TenantConfigOption func(cfg *splitterpb.Tenant_Config)

// TenantConfig holds tenant configuration.
type TenantConfig struct {
	pb *splitterpb.Tenant_Config
}

func NewTenantConfig(opts ...TenantConfigOption) TenantConfig {
	pb := &splitterpb.Tenant_Config{}
	for _, fn := range opts {
		fn(pb)
	}
	return WrapTenantConfig(pb)
}

func UpdateTenantConfig(tenant Tenant, opts ...TenantConfigOption) (TenantConfig, error) {
	pb := UnwrapTenant(tenant).Config
	if pb == nil {
		pb = &splitterpb.Tenant_Config{}
	}
	pb = protox.Clone(pb)
	for _, fn := range opts {
		fn(pb)
	}
	if _, err := UpdateTenant(tenant, WithTenantConfig(WrapTenantConfig(pb))); err != nil {
		return TenantConfig{}, err
	}
	return WrapTenantConfig(pb), nil
}

func WrapTenantConfig(pb *splitterpb.Tenant_Config) TenantConfig {
	return TenantConfig{pb: pb}
}

func UnwrapTenantConfig(cfg TenantConfig) *splitterpb.Tenant_Config {
	return cfg.pb
}

func (c TenantConfig) Equals(o TenantConfig) bool {
	return protox.Equal(c.pb, o.pb)
}

func (c TenantConfig) String() string {
	return protox.MarshalTextString(c.pb)
}

// TenantInfo captures the full tenant information.
type TenantInfo struct {
	pb *splitterpb.TenantInfo
}

func WrapTenantInfo(pb *splitterpb.TenantInfo) TenantInfo {
	return TenantInfo{pb: pb}
}

func UnwrapTenantInfo(t TenantInfo) *splitterpb.TenantInfo {
	return t.pb
}

func NewTenantInfo(tenant Tenant, version Version, now time.Time) TenantInfo {
	return WrapTenantInfo(&splitterpb.TenantInfo{
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
	return protox.MarshalTextString(t.pb)
}
