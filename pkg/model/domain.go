package model

import (
	"go.atoms.co/slicex"
	"go.atoms.co/lib/uuidx"
	"go.atoms.co/splitter/pb"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"
	"strings"
	"time"
)

type Version int64

type DomainName string

type QualifiedDomainName struct {
	Service QualifiedServiceName
	Domain  DomainName
}

func ParseQualifiedDomainNameStr(name string) (QualifiedDomainName, bool) {
	parts := slicex.Map(strings.Split(name, "/"), strings.TrimSpace)
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return QualifiedDomainName{}, false
	}
	return QualifiedDomainName{
		Service: QualifiedServiceName{
			Tenant:  TenantName(parts[0]),
			Service: ServiceName(parts[1]),
		},
		Domain: DomainName(parts[2]),
	}, true
}

func ParseQualifiedDomainName(pb *public_v1.QualifiedDomainName) (QualifiedDomainName, error) {
	if pb.GetName() == "" {
		return QualifiedDomainName{}, fmt.Errorf("invalid domain name: %v", proto.MarshalTextString(pb))
	}
	service, err := ParseQualifiedServiceName(pb.GetService())
	if err != nil {
		return QualifiedDomainName{}, err
	}

	return QualifiedDomainName{
		Service: service,
		Domain:  DomainName(pb.GetName()),
	}, nil
}

func (n QualifiedDomainName) ToProto() *public_v1.QualifiedDomainName {
	return &public_v1.QualifiedDomainName{
		Service: n.Service.ToProto(),
		Name:    string(n.Domain),
	}
}

func (n QualifiedDomainName) String() string {
	return fmt.Sprintf("%v/%v", n.Service, n.Domain)
}

type DomainType = public_v1.Domain_Type

const (
	Unit     = public_v1.Domain_UNIT
	Global   = public_v1.Domain_GLOBAL
	Regional = public_v1.Domain_REGIONAL
)

type DomainState = public_v1.Domain_State

const (
	DomainActive    = public_v1.Domain_ACTIVE
	DomainSuspended = public_v1.Domain_SUSPENDED
)

func ParseDomainState(str string) (DomainState, bool) {
	v, ok := public_v1.Domain_State_value[strings.ToUpper(str)]
	return DomainState(v), ok && v != 0
}

type DomainOption func(tenant *public_v1.Domain)

func WithDomainState(state public_v1.Domain_State) DomainOption {
	return func(subscription *public_v1.Domain) {
		subscription.State = state
	}
}

func WithDomainConfig(cfg DomainConfig) DomainOption {
	return func(domain *public_v1.Domain) {
		domain.Config = UnwrapDomainConfig(cfg)
	}
}

type Domain struct {
	pb *public_v1.Domain
}

func NewDomain(name QualifiedDomainName, t DomainType, now time.Time, opts ...DomainOption) (Domain, error) {
	pb := &public_v1.Domain{
		Name:    name.ToProto(),
		Type:    t,
		State:   DomainActive,
		Config:  &public_v1.Domain_Config{},
		Created: timestamppb.New(now),
	}
	for _, fn := range opts {
		fn(pb)
	}
	return ParseDomain(pb)
}

func ParseDomain(pb *public_v1.Domain) (Domain, error) {
	if err := validateDomain(pb); err != nil {
		return Domain{}, fmt.Errorf("invalid domain: %w", err)
	}
	return Domain{pb: proto.Clone(pb).(*public_v1.Domain)}, nil
}

func validateDomain(pb *public_v1.Domain) error {
	return nil // TODO(jhhurwitz): 08/18/2023 Actually validate
}

func UpdateDomain(domain Domain, opts ...DomainOption) (Domain, error) {
	upd := proto.Clone(domain.pb).(*public_v1.Domain)
	for _, fn := range opts {
		fn(upd)
	}
	return ParseDomain(upd)
}

func WrapDomain(pb *public_v1.Domain) Domain {
	return Domain{pb: pb}
}

func UnwrapDomain(domain Domain) *public_v1.Domain {
	return domain.pb
}

func (t Domain) Name() QualifiedDomainName {
	ret, _ := ParseQualifiedDomainName(t.pb.GetName())
	return ret
}

func (t Domain) Config() DomainConfig {
	return WrapDomainConfig(t.pb.GetConfig())
}

func (t Domain) Regions() ([]Region, bool) {
	regions := slicex.Map(t.pb.GetConfig().GetRegions(), func(r string) Region {
		return Region(r)
	})
	return regions, len(regions) > 0
}

func (t Domain) Type() DomainType {
	return t.pb.GetType()
}

func (t Domain) State() DomainState {
	return t.pb.GetState()
}

func (t Domain) Equals(t1 Domain) bool {
	return proto.Equal(t.pb, t1.pb)
}

func (t Domain) String() string {
	return proto.MarshalTextString(t.pb)
}

type DomainConfigOption func(cfg *public_v1.Domain_Config)

func WithDomainPlacement(placement string) DomainConfigOption {
	return func(cfg *public_v1.Domain_Config) {
		cfg.Placement = placement
	}
}

func WithDomainRegions(regions ...string) DomainConfigOption {
	return func(cfg *public_v1.Domain_Config) {
		cfg.Regions = regions
	}
}

func WithDomainShardingPolicy(policy ShardingPolicy) DomainConfigOption {
	return func(cfg *public_v1.Domain_Config) {
		cfg.ShardingPolicy = UnwrapShardingPolicy(policy)
	}
}

func WithDomainAntiAffinity(domains ...QualifiedDomainName) DomainConfigOption {
	return func(cfg *public_v1.Domain_Config) {
		cfg.AntiAffinity = slicex.Map(domains, QualifiedDomainName.ToProto)
	}
}

// DomainConfig holds domain configuration.
type DomainConfig struct {
	pb *public_v1.Domain_Config
}

func NewDomainConfig(opts ...DomainConfigOption) DomainConfig {
	pb := &public_v1.Domain_Config{}
	for _, fn := range opts {
		fn(pb)
	}
	return WrapDomainConfig(pb)
}

func UpdateDomainConfig(domain Domain, opts ...DomainConfigOption) (DomainConfig, error) {
	pb := UnwrapDomain(domain).Config
	if pb == nil {
		pb = &public_v1.Domain_Config{}
	}
	pb = proto.Clone(pb).(*public_v1.Domain_Config)
	for _, fn := range opts {
		fn(pb)
	}
	if _, err := UpdateDomain(domain, WithDomainConfig(WrapDomainConfig(pb))); err != nil {
		return DomainConfig{}, err
	}
	return WrapDomainConfig(pb), nil
}

func WrapDomainConfig(pb *public_v1.Domain_Config) DomainConfig {
	return DomainConfig{pb: pb}
}

func UnwrapDomainConfig(cfg DomainConfig) *public_v1.Domain_Config {
	return cfg.pb
}

func (c DomainConfig) Placement() (PlacementName, bool) {
	return PlacementName(c.pb.GetPlacement()), c.pb.GetPlacement() != ""
}

func (c DomainConfig) ShardingPolicy() ShardingPolicy {
	return WrapShardingPolicy(c.pb.GetShardingPolicy())
}

func (c DomainConfig) Regions() []Region {
	return slicex.Map(c.pb.GetRegions(), func(r string) Region { return Region(r) })
}

// Key is a UUID key for a domain or placement.
type Key uuid.UUID

var (
	ZeroKey = Key(uuidx.Min)
	MaxKey  = Key(uuidx.Max)
)

func ParseKey(key string) (Key, error) {
	ret, err := uuid.Parse(key)
	if err != nil {
		return Key{}, fmt.Errorf("invalid uuid '%v': %v", key, err)
	}
	return Key(ret), nil
}

func MustParseKey(key string) Key {
	ret, err := ParseKey(key)
	if err != nil {
		panic(err)
	}
	return ret
}

func (k Key) Less(o Key) bool {
	return uuidx.Less(uuid.UUID(k), uuid.UUID(o))
}

func (k Key) String() string {
	return uuid.UUID(k).String()
}

// DomainKey specifies a domain element for an implicit domain.
type DomainKey struct {
	Region Region // if REGIONAL domain
	Key    Key    // if REGIONAL/GLOBAL
}

var (
	ZeroDomainKey = DomainKey{}
)

func (k DomainKey) String() string {
	if k.Region != "" {
		return fmt.Sprintf("%v/%v", k.Region, k.Key)
	}
	if k.Key != ZeroKey {
		return k.Key.String()
	}
	return "*"
}

// QualifiedDomainKey fully specifies a domain element.
type QualifiedDomainKey struct {
	Domain QualifiedDomainName
	Key    DomainKey
}

func (k QualifiedDomainKey) String() string {
	return fmt.Sprintf("%v:%v", k.Domain, k.Key)
}
