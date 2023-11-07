package model

import (
	"go.atoms.co/slicex"
	"go.atoms.co/splitter/pb"
	"fmt"
	"github.com/golang/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"strings"
	"time"
)

type ServiceName string

type QualifiedServiceName struct {
	Tenant  TenantName
	Service ServiceName
}

func ParseQualifiedServiceNameStr(name string) (QualifiedServiceName, bool) {
	parts := slicex.Map(strings.Split(name, "/"), strings.TrimSpace)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return QualifiedServiceName{}, false
	}
	return QualifiedServiceName{
		Tenant:  TenantName(parts[0]),
		Service: ServiceName(parts[1]),
	}, true
}

func ParseQualifiedServiceName(pb *public_v1.QualifiedServiceName) (QualifiedServiceName, error) {
	if pb.GetTenant() == "" || pb.GetService() == "" {
		return QualifiedServiceName{}, fmt.Errorf("invalid service name: %v", proto.MarshalTextString(pb))
	}
	return QualifiedServiceName{
		Tenant:  TenantName(pb.GetTenant()),
		Service: ServiceName(pb.GetService()),
	}, nil
}

func (n QualifiedServiceName) ToProto() *public_v1.QualifiedServiceName {
	return &public_v1.QualifiedServiceName{
		Tenant:  string(n.Tenant),
		Service: string(n.Service),
	}
}

func (n QualifiedServiceName) String() string {
	return fmt.Sprintf("%v/%v", n.Tenant, n.Service)
}

type ServiceOption func(tenant *public_v1.Service)

func WithServiceConfig(cfg ServiceConfig) ServiceOption {
	return func(tenant *public_v1.Service) {
		tenant.Config = UnwrapServiceConfig(cfg)
	}
}

// Service represents a top-level namespace.
type Service struct {
	pb *public_v1.Service
}

func NewService(name QualifiedServiceName, now time.Time, opts ...ServiceOption) (Service, error) {
	pb := &public_v1.Service{
		Name:    name.ToProto(),
		Config:  &public_v1.Service_Config{},
		Created: timestamppb.New(now),
	}
	for _, fn := range opts {
		fn(pb)
	}
	return ParseService(pb)
}

func ParseService(pb *public_v1.Service) (Service, error) {
	if err := validateService(pb); err != nil {
		return Service{}, fmt.Errorf("invalid tenant: %w", err)
	}
	return Service{pb: proto.Clone(pb).(*public_v1.Service)}, nil
}

func validateService(pb *public_v1.Service) error {
	return nil // TODO(jhhurwitz): 08/18/2023 Actually validate
}

func UpdateService(tenant Service, opts ...ServiceOption) (Service, error) {
	upd := proto.Clone(tenant.pb).(*public_v1.Service)
	for _, fn := range opts {
		fn(upd)
	}
	return ParseService(upd)
}

func WrapService(tenant *public_v1.Service) Service {
	return Service{pb: tenant}
}

func UnwrapService(tenant Service) *public_v1.Service {
	return tenant.pb
}

func (t Service) Name() QualifiedServiceName {
	ret, _ := ParseQualifiedServiceName(t.pb.GetName())
	return ret
}

func (t Service) Config() ServiceConfig {
	return WrapServiceConfig(t.pb.GetConfig())
}

// Region returns a region preference for coordinator and UNIT/GLOBAL domains. Optional.
func (t Service) Region() (Region, bool) {
	r := t.pb.GetConfig().GetRegion()
	return Region(r), r != ""
}

func (t Service) Equals(t1 Service) bool {
	return proto.Equal(t.pb, t1.pb)
}

func (t Service) String() string {
	return proto.MarshalTextString(t.pb)
}

type ServiceConfigOption func(cfg *public_v1.Service_Config)

func WithServiceRegion(region Region) ServiceConfigOption {
	return func(cfg *public_v1.Service_Config) {
		cfg.Region = string(region)
	}
}

func WithServiceDefaultShardingPolicy(policy ShardingPolicy) ServiceConfigOption {
	return func(cfg *public_v1.Service_Config) {
		cfg.DefaultShardingPolicy = UnwrapShardingPolicy(policy)
	}
}

// ServiceConfig holds tenant configuration.
type ServiceConfig struct {
	pb *public_v1.Service_Config
}

func NewServiceConfig(region Region, opts ...ServiceConfigOption) ServiceConfig {
	pb := &public_v1.Service_Config{
		Region: string(region),
	}
	for _, fn := range opts {
		fn(pb)
	}
	return WrapServiceConfig(pb)
}

func UpdateServiceConfig(tenant Service, opts ...ServiceConfigOption) (ServiceConfig, error) {
	pb := UnwrapService(tenant).Config
	if pb == nil {
		pb = &public_v1.Service_Config{}
	}
	pb = proto.Clone(pb).(*public_v1.Service_Config)
	for _, fn := range opts {
		fn(pb)
	}
	if _, err := UpdateService(tenant, WithServiceConfig(WrapServiceConfig(pb))); err != nil {
		return ServiceConfig{}, err
	}
	return WrapServiceConfig(pb), nil
}

func WrapServiceConfig(pb *public_v1.Service_Config) ServiceConfig {
	return ServiceConfig{pb: pb}
}

func UnwrapServiceConfig(cfg ServiceConfig) *public_v1.Service_Config {
	return cfg.pb
}

func (c ServiceConfig) DefaultShardingPolicy() ShardingPolicy {
	return WrapShardingPolicy(c.pb.GetDefaultShardingPolicy())
}

// ServiceInfo captures the full service information.
type ServiceInfo struct {
	pb *public_v1.ServiceInfo
}

func WrapServiceInfo(pb *public_v1.ServiceInfo) ServiceInfo {
	return ServiceInfo{pb: pb}
}

func UnwrapServiceInfo(t ServiceInfo) *public_v1.ServiceInfo {
	return t.pb
}

func NewServiceInfo(service Service, version Version, now time.Time) ServiceInfo {
	return WrapServiceInfo(&public_v1.ServiceInfo{
		Service:   UnwrapService(service),
		Version:   int64(version),
		Timestamp: timestamppb.New(now),
	})
}

func (t ServiceInfo) Name() QualifiedServiceName {
	return t.Service().Name()
}

func (t ServiceInfo) Service() Service {
	return WrapService(t.pb.GetService())
}

func (t ServiceInfo) Version() Version {
	return Version(t.pb.GetVersion())
}

func (t ServiceInfo) Timestamp() time.Time {
	return t.pb.GetTimestamp().AsTime()
}

func (t ServiceInfo) String() string {
	return proto.MarshalTextString(t.pb)
}

// ServiceInfoEx captures the full service information and associated Domains.
type ServiceInfoEx struct {
	pb *public_v1.ServiceInfoEx
}

func WrapServiceInfoEx(pb *public_v1.ServiceInfoEx) ServiceInfoEx {
	return ServiceInfoEx{pb: pb}
}

func UnwrapServiceInfoEx(t ServiceInfoEx) *public_v1.ServiceInfoEx {
	return t.pb
}

func NewServiceInfoEx(service ServiceInfo, domains []Domain) ServiceInfoEx {
	return WrapServiceInfoEx(&public_v1.ServiceInfoEx{
		Service: UnwrapServiceInfo(service),
		Domains: slicex.Map(domains, UnwrapDomain),
	})
}

func (t ServiceInfoEx) Name() QualifiedServiceName {
	return t.Info().Name()
}

func (t ServiceInfoEx) Info() ServiceInfo {
	return WrapServiceInfo(t.pb.GetService())
}

func (t ServiceInfoEx) Domains() []Domain {
	return slicex.Map(t.pb.GetDomains(), WrapDomain)
}

func (t ServiceInfoEx) Domain(name DomainName) (Domain, bool) {
	s, ok := slicex.First(t.pb.Domains, func(d *public_v1.Domain) bool {
		return d.GetName().GetName() == string(name)
	})
	return WrapDomain(s), ok
}
func (t ServiceInfoEx) String() string {
	return proto.MarshalTextString(t.pb)
}
