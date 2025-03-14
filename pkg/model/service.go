package model

import (
	"fmt"
	"strings"
	"time"

	"github.com/golang/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"go.atoms.co/splitter/lib/service/location"
	"go.atoms.co/lib/mapx"
	"go.atoms.co/slicex"
	"go.atoms.co/splitter/pb"
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

func MustParseQualifiedServiceNameStr(name string) QualifiedServiceName {
	n, ok := ParseQualifiedServiceNameStr(name)
	if !ok {
		panic(fmt.Sprintf("invalid name: %v", name))
	}
	return n
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

type ServiceOption func(service *public_v1.Service)

func WithServiceOperational(t ServiceOperational) ServiceOption {
	return func(service *public_v1.Service) {
		service.Operational = UnwrapServiceOperational(t)
	}
}

func WithServiceConfig(cfg ServiceConfig) ServiceOption {
	return func(service *public_v1.Service) {
		service.Config = UnwrapServiceConfig(cfg)
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
		return Service{}, fmt.Errorf("invalid service: %w", err)
	}
	return Service{pb: proto.Clone(pb).(*public_v1.Service)}, nil
}

func validateService(pb *public_v1.Service) error {
	return nil // TODO(jhhurwitz): 08/18/2023 Actually validate
}

func UpdateService(service Service, opts ...ServiceOption) (Service, error) {
	upd := proto.Clone(service.pb).(*public_v1.Service)
	for _, fn := range opts {
		fn(upd)
	}
	return ParseService(upd)
}

func WrapService(service *public_v1.Service) Service {
	return Service{pb: service}
}

func UnwrapService(service Service) *public_v1.Service {
	return service.pb
}

func (t Service) Name() QualifiedServiceName {
	ret, _ := ParseQualifiedServiceName(t.pb.GetName())
	return ret
}

func (t Service) Operational() ServiceOperational {
	return WrapServiceOperational(t.pb.GetOperational())
}

func (t Service) Config() ServiceConfig {
	return WrapServiceConfig(t.pb.GetConfig())
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

func WithLocalityOverrides(overrides map[location.Region]location.Region) ServiceConfigOption {
	return func(cfg *public_v1.Service_Config) {
		cfg.Overrides = mapx.MapToSlice(overrides, func(shard location.Region, consumer location.Region) *public_v1.Service_Config_LocalityOverride {
			return &public_v1.Service_Config_LocalityOverride{
				ShardRegion:    string(shard),
				ConsumerRegion: string(consumer),
			}
		})
	}
}

// ServiceConfig holds service configuration.
type ServiceConfig struct {
	pb *public_v1.Service_Config
}

func NewServiceConfig(opts ...ServiceConfigOption) ServiceConfig {
	pb := &public_v1.Service_Config{}
	for _, fn := range opts {
		fn(pb)
	}
	return WrapServiceConfig(pb)
}

func UpdateServiceConfig(service Service, opts ...ServiceConfigOption) (ServiceConfig, error) {
	pb := UnwrapService(service).Config
	if pb == nil {
		pb = &public_v1.Service_Config{}
	}
	pb = proto.Clone(pb).(*public_v1.Service_Config)
	for _, fn := range opts {
		fn(pb)
	}
	if _, err := UpdateService(service, WithServiceConfig(WrapServiceConfig(pb))); err != nil {
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

func (c ServiceConfig) Region() Region {
	return Region(c.pb.GetRegion())
}

func (c ServiceConfig) DefaultShardingPolicy() ShardingPolicy {
	return WrapShardingPolicy(c.pb.GetDefaultShardingPolicy())
}

func (c ServiceConfig) Overrides() map[location.Region]location.Region {
	return mapx.MapNew(c.pb.GetOverrides(), func(t *public_v1.Service_Config_LocalityOverride) (location.Region, location.Region) {
		return location.Region(t.GetShardRegion()), location.Region(t.GetConsumerRegion())
	})
}

func (c ServiceConfig) Equals(c1 ServiceConfig) bool {
	return proto.Equal(c.pb, c1.pb)
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

func (t ServiceInfoEx) Service() Service {
	return WrapService(t.pb.GetService().GetService())
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

func (t ServiceInfoEx) Equals(o ServiceInfoEx) bool {
	return proto.Equal(t.pb, o.pb)
}

func (t ServiceInfoEx) String() string {
	return proto.MarshalTextString(t.pb)
}
