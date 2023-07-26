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

type DomainOption func(tenant *public_v1.Domain)

type Domain struct {
	pb *public_v1.Domain
}

func NewDomain(name QualifiedDomainName, t DomainType, now time.Time, opts ...DomainOption) Domain {
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
	return WrapDomain(pb)
}

func UpdateDomain(domain Domain, opts ...DomainOption) Domain {
	upd := proto.Clone(domain.pb).(*public_v1.Domain)
	for _, fn := range opts {
		fn(upd)
	}
	return WrapDomain(upd)
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

// Key is a UUID key for a domain or placement.
type Key uuid.UUID

var (
	ZeroKey = Key(uuid.Nil)
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

type QualifiedKey struct {
	Domain QualifiedDomainName
	Key    Key
}
