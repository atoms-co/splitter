package model

import (
	"go.atoms.co/splitter/lib/service/location"
	"go.atoms.co/splitter/lib/service/session"
	"go.atoms.co/slicex"
	"go.atoms.co/lib/uuidx"
	"go.atoms.co/splitter/pb"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"
	"time"
)

type InstanceID = location.InstanceID

type Instance struct {
	pb *public_v1.Instance
}

func WrapInstance(pb *public_v1.Instance) Instance {
	return Instance{pb: pb}
}

func UnwrapInstance(instance Instance) *public_v1.Instance {
	return instance.pb
}

func (i Instance) Location() location.Instance {
	return location.WrapInstance(i.pb.GetClient())
}

func (i Instance) Endpoint() string {
	return i.pb.GetEndpoint()
}

func (i Instance) String() string {
	return fmt.Sprintf("%v=%v", i.Location(), i.Endpoint())
}

type Shard struct {
	Region Region
	Domain QualifiedDomainName
	To     Key
	From   Key
}

// NewShards returns N shards for a region and domain, uniformly split.
func NewShards(r Region, domain QualifiedDomainName, n int) []Shard {
	list, err := uuidx.Split(uuidx.Domain, n)
	if err != nil {
		panic(err)
	}
	return slicex.Map(list, func(s uuidx.Range) Shard {
		return Shard{
			Region: r,
			Domain: domain,
			From:   Key(s.From()),
			To:     Key(s.To()),
		}
	})
}

func ParseShard(pb *public_v1.Shard) (Shard, error) {
	if pb.GetDomain() == nil {
		return Shard{}, fmt.Errorf("missing domain: %v", proto.MarshalTextString(pb))
	}
	to, err := ParseKey(pb.GetTo())
	if err != nil {
		return Shard{}, fmt.Errorf("invalid to: %v", proto.MarshalTextString(pb))
	}
	from, err := ParseKey(pb.GetFrom())
	if err != nil {
		return Shard{}, fmt.Errorf("invalid from: %v", proto.MarshalTextString(pb))
	}
	domain, err := ParseQualifiedDomainName(pb.GetDomain())
	if err != nil {
		return Shard{}, fmt.Errorf("invalid domain: %w", err)
	}
	return Shard{
		Region: Region(pb.GetRegion()),
		Domain: domain,
		To:     to,
		From:   from,
	}, nil
}

func (s Shard) ShardDomain() QualifiedDomainName {
	return s.Domain
}

func (s Shard) Equals(o Shard) bool {
	return s == o
}

func (s Shard) ToProto() *public_v1.Shard {
	return &public_v1.Shard{
		Region: string(s.Region),
		Domain: s.Domain.ToProto(),
		To:     s.To.String(),
		From:   s.From.String(),
	}
}

func (s Shard) String() string {
	if s.Region != "" {
		return fmt.Sprintf("shard[region=%v, domain=%v, from: %v, to: %v]", s.Region, s.Domain, s.From, s.To)
	}
	return fmt.Sprintf("shard[domain=%v, from: %v, to: %v]", s.Domain, s.From, s.To)
}

type GrantState = public_v1.Grant_State

var (
	InvalidGrant   = public_v1.Grant_UNKNOWN
	AllocatedGrant = public_v1.Grant_ALLOCATED
	ActiveGrant    = public_v1.Grant_ACTIVE
	RevokedGrant   = public_v1.Grant_REVOKED
)

type GrantID uuid.UUID

func NewGrantID() GrantID {
	return GrantID(uuid.New())
}

func ParseGrantID(id string) (GrantID, error) {
	ret, err := uuid.Parse(id)
	return GrantID(ret), err
}

func MustParseGrantID(id string) GrantID {
	ret, err := uuid.Parse(id)
	if err != nil {
		panic(err)
	}
	return GrantID(ret)
}

func (a GrantID) String() string {
	return uuid.UUID(a).String()
}

type Grant struct {
	ID       GrantID
	Shard    Shard
	State    GrantState
	Lease    time.Time
	Assigned time.Time
}

func ParseGrant(pb *public_v1.Grant) (Grant, error) {
	allocID, err := ParseGrantID(pb.GetId())
	if err != nil {
		return Grant{}, fmt.Errorf("invalid grant id: %v: %w", proto.MarshalTextString(pb), err)
	}
	shard, err := ParseShard(pb.GetShard())
	if err != nil {
		return Grant{}, fmt.Errorf("invalid shard: %v: %w", proto.MarshalTextString(pb), err)
	}
	return Grant{
		ID:       allocID,
		Shard:    shard,
		State:    pb.GetState(),
		Lease:    pb.GetLease().AsTime(),
		Assigned: pb.GetAssigned().AsTime(),
	}, nil
}

func (g Grant) ToProto() *public_v1.Grant {
	return &public_v1.Grant{
		Id:       g.ID.String(),
		Shard:    g.Shard.ToProto(),
		State:    g.State,
		Lease:    timestamppb.New(g.Lease),
		Assigned: timestamppb.New(g.Assigned),
	}
}

func (g Grant) String() string {
	return fmt.Sprintf("%v[shard=%v, state=%v, lease=%v, assinged=%v]", g.ID, g.Shard, g.State, g.Lease, g.Assigned)
}

type Assignment struct {
	pb *public_v1.Assignment
}

func WrapAssignment(pb *public_v1.Assignment) Assignment {
	return Assignment{pb: pb}
}

func UnwrapAssignment(a Assignment) *public_v1.Assignment {
	return a.pb
}

func (a Assignment) Instance() Instance {
	return WrapInstance(a.pb.GetInstance())
}

func (a Assignment) ParseGrants() ([]Grant, error) {
	return slicex.TryMap(a.pb.GetGrants(), ParseGrant)
}

type RegisterMessage struct {
	pb *public_v1.Register
}

func WrapRegisterMessage(pb *public_v1.Register) RegisterMessage {
	return RegisterMessage{pb: pb}
}

func UnwrapRegisterMessage(m RegisterMessage) *public_v1.Register {
	return m.pb
}

func (m RegisterMessage) TenantName() TenantName {
	return TenantName(m.pb.Tenant)
}

func (m RegisterMessage) Instance() Instance {
	return WrapInstance(m.pb.GetInstance())
}

func (m RegisterMessage) ParseActive() ([]Grant, error) {
	return slicex.TryMap(m.pb.GetActive(), ParseGrant)
}

func (m RegisterMessage) String() string {
	return proto.MarshalTextString(m.pb)
}

type ReleasedMessage struct {
	pb *public_v1.Released
}

func WrapReleasedMessage(pb *public_v1.Released) ReleasedMessage {
	return ReleasedMessage{pb: pb}
}

func UnwrapReleasedMessage(m ReleasedMessage) *public_v1.Released {
	return m.pb
}

func (m ReleasedMessage) ParseGrants() ([]GrantID, error) {
	return slicex.TryMap(m.pb.GetGrants(), ParseGrantID)
}

type ConsumerMessage struct {
	pb *public_v1.ConsumerMessage
}

func WrapConsumerMessage(pb *public_v1.ConsumerMessage) ConsumerMessage {
	return ConsumerMessage{pb: pb}
}

func UnwrapConsumerMessage(m ConsumerMessage) *public_v1.ConsumerMessage {
	return m.pb
}

func NewConsumerSessionMessage(m session.Message) ConsumerMessage {
	return WrapConsumerMessage(&public_v1.ConsumerMessage{
		Msg: &public_v1.ConsumerMessage_Session{
			Session: session.UnwrapMessage(m),
		},
	})
}

func (m ConsumerMessage) IsRegister() bool {
	return m.pb.GetRegister() != nil
}

func (m ConsumerMessage) Register() (RegisterMessage, bool) {
	if !m.IsRegister() {
		return RegisterMessage{}, false
	}
	return WrapRegisterMessage(m.pb.GetRegister()), true
}

func (m ConsumerMessage) IsDeregister() bool {
	return m.pb.GetRegister() != nil
}

func (m ConsumerMessage) IsReleased() bool {
	return m.pb.GetReleased() != nil
}

func (m ConsumerMessage) Released() (ReleasedMessage, bool) {
	if !m.IsRegister() {
		return ReleasedMessage{}, false
	}
	return WrapReleasedMessage(m.pb.GetReleased()), true
}

func (m ConsumerMessage) String() string {
	return proto.MarshalTextString(m.pb)
}
