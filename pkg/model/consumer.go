package model

import (
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

type ConsumerID = InstanceID
type Consumer = Instance

type Shard struct {
	Domain QualifiedDomainName
	Type   DomainType
	Region Region
	To     Key
	From   Key
}

// NewShards returns N shards for a region and domain, uniformly split.
func NewShards(domain QualifiedDomainName, dtype DomainType, r Region, n int) []Shard {
	list, err := uuidx.Split(uuidx.Domain, n)
	if err != nil {
		panic(err)
	}
	return slicex.Map(list, func(s uuidx.Range) Shard {
		return Shard{
			Type:   dtype,
			Domain: domain,
			Region: r,
			From:   Key(s.From()),
			To:     Key(s.To()),
		}
	})
}

func ParseShard(pb *public_v1.Shard) (Shard, error) {
	if pb.GetDomain() == nil {
		return Shard{}, fmt.Errorf("missing domain: %v", proto.MarshalTextString(pb))
	}
	domain, err := ParseQualifiedDomainName(pb.GetDomain())
	if err != nil {
		return Shard{}, fmt.Errorf("invalid domain: %w", err)
	}
	switch pb.GetType() {
	case Unit:
		return Shard{
			Domain: domain,
			Type:   pb.GetType(),
		}, nil
	case Global:
		to, err := ParseKey(pb.GetTo())
		if err != nil {
			return Shard{}, fmt.Errorf("invalid to: %v", proto.MarshalTextString(pb))
		}
		from, err := ParseKey(pb.GetFrom())
		if err != nil {
			return Shard{}, fmt.Errorf("invalid from: %v", proto.MarshalTextString(pb))
		}
		return Shard{
			Domain: domain,
			Type:   pb.GetType(),
			To:     to,
			From:   from,
		}, nil
	case Regional:
		to, err := ParseKey(pb.GetTo())
		if err != nil {
			return Shard{}, fmt.Errorf("invalid to: %v", proto.MarshalTextString(pb))
		}
		from, err := ParseKey(pb.GetFrom())
		if err != nil {
			return Shard{}, fmt.Errorf("invalid from: %v", proto.MarshalTextString(pb))
		}
		return Shard{
			Domain: domain,
			Type:   pb.GetType(),
			Region: Region(pb.GetRegion()),
			To:     to,
			From:   from,
		}, nil
	default:
		return Shard{}, fmt.Errorf("invalid shard domain type: %v", pb.GetType())
	}
}

func (s Shard) Contains(key QualifiedDomainKey) bool {
	switch s.Type {
	case Unit:
		return s.Domain == key.Domain
	case Global:
		if s.Domain != key.Domain {
			return false
		}
		r, _ := uuidx.NewRange(uuid.UUID(s.From), uuid.UUID(s.To))
		return r.Contains(uuid.UUID(key.Key.Key))
	case Regional:
		if s.Domain != key.Domain || s.Region != key.Key.Region {
			return false
		}
		r, _ := uuidx.NewRange(uuid.UUID(s.From), uuid.UUID(s.To))
		return r.Contains(uuid.UUID(key.Key.Key))
	default:
		return false
	}
}

func (s Shard) ToProto() *public_v1.Shard {
	return &public_v1.Shard{
		Region: string(s.Region),
		Type:   s.Type,
		Domain: s.Domain.ToProto(),
		To:     s.To.String(),
		From:   s.From.String(),
	}
}

func (s Shard) String() string {
	switch s.Type {
	case Unit:
		return fmt.Sprintf("%v", s.Domain)
	case Global:
		return fmt.Sprintf("%v[%v-%v)", s.Domain, s.To, s.From)
	case Regional:
		return fmt.Sprintf("%v@%v[%v-%v)", s.Domain, s.Region, s.To, s.From)
	default:
		return "invalid-shard"
	}
}

type GrantState = public_v1.GrantState

var (
	InvalidGrant   = public_v1.GrantState_UNKNOWN
	AllocatedGrant = public_v1.GrantState_ALLOCATED
	ActiveGrant    = public_v1.GrantState_ACTIVE
	RevokedGrant   = public_v1.GrantState_REVOKED
)

func IsAllocatedGrant(state GrantState) bool {
	return state == AllocatedGrant
}

func IsActiveGrant(state GrantState) bool {
	return state == ActiveGrant
}

func IsRevokedGrant(state GrantState) bool {
	return state == RevokedGrant
}

type GrantID string

type Grant struct {
	ID       GrantID
	Shard    Shard
	State    GrantState
	Lease    time.Time
	Assigned time.Time
}

func ParseGrant(pb *public_v1.Grant) (Grant, error) {
	shard, err := ParseShard(pb.GetShard())
	if err != nil {
		return Grant{}, fmt.Errorf("invalid shard: %v: %w", proto.MarshalTextString(pb), err)
	}
	return Grant{
		ID:       GrantID(pb.GetId()),
		Shard:    shard,
		State:    pb.GetState(),
		Lease:    pb.GetLease().AsTime(),
		Assigned: pb.GetAssigned().AsTime(),
	}, nil
}

func (g Grant) ToProto() *public_v1.Grant {
	return &public_v1.Grant{
		Id:       string(g.ID),
		Shard:    g.Shard.ToProto(),
		State:    g.State,
		Lease:    timestamppb.New(g.Lease),
		Assigned: timestamppb.New(g.Assigned),
	}
}

func (g Grant) String() string {
	return fmt.Sprintf("%v[shard=%v, state=%v, lease=%v, assinged=%v]", g.ID, g.Shard, g.State, g.Lease, g.Assigned)
}

type GrantInfo struct {
	ID    GrantID
	Shard Shard
	State GrantState
}

func ParseGrantInfo(pb *public_v1.ClusterMessage_GrantInfo) (GrantInfo, error) {
	shard, err := ParseShard(pb.GetShard())
	if err != nil {
		return GrantInfo{}, fmt.Errorf("invalid shard: %v: %w", proto.MarshalTextString(pb), err)
	}
	return GrantInfo{
		ID:    GrantID(pb.GetId()),
		Shard: shard,
		State: pb.GetState(),
	}, nil
}

func (g GrantInfo) ToProto() *public_v1.ClusterMessage_GrantInfo {
	return &public_v1.ClusterMessage_GrantInfo{
		Id:    string(g.ID),
		Shard: g.Shard.ToProto(),
		State: g.State,
	}
}

func (g GrantInfo) String() string {
	return fmt.Sprintf("%v[shard=%v, state=%v]", g.ID, g.Shard, g.State)
}

type Assignment struct {
	pb *public_v1.ClusterMessage_Assignment
}

func WrapAssignment(pb *public_v1.ClusterMessage_Assignment) Assignment {
	return Assignment{pb: pb}
}

func UnwrapAssignment(a Assignment) *public_v1.ClusterMessage_Assignment {
	return a.pb
}

func NewAssignment(consumer Consumer, grants ...GrantInfo) Assignment {
	return Assignment{
		pb: &public_v1.ClusterMessage_Assignment{
			Consumer: UnwrapInstance(consumer),
			Grants:   slicex.Map(grants, GrantInfo.ToProto),
		},
	}
}

func (a Assignment) Consumer() Consumer {
	return WrapInstance(a.pb.GetConsumer())
}

func (a Assignment) ParseGrants() ([]GrantInfo, error) {
	return slicex.TryMap(a.pb.GetGrants(), ParseGrantInfo)
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

func NewRegisterMessage(consumer Consumer, service QualifiedServiceName, domains []QualifiedDomainName, grants []Grant) RegisterMessage {
	return WrapRegisterMessage(&public_v1.Register{
		Consumer: UnwrapInstance(consumer),
		Service:  service.ToProto(),
		Domains:  slicex.Map(domains, QualifiedDomainName.ToProto),
		Active:   slicex.Map(grants, Grant.ToProto),
	})
}

func (m RegisterMessage) Service() QualifiedServiceName {
	ret, _ := ParseQualifiedServiceName(m.pb.GetService())
	return ret
}

func (m RegisterMessage) Consumer() Consumer {
	return WrapInstance(m.pb.GetConsumer())
}

func (m RegisterMessage) ParseActive() ([]Grant, error) {
	return slicex.TryMap(m.pb.GetActive(), ParseGrant)
}

func (m RegisterMessage) String() string {
	return proto.MarshalTextString(m.pb)
}

type DeregisterMessage struct {
	pb *public_v1.Deregister
}

func WrapDeregisterMessage(pb *public_v1.Deregister) DeregisterMessage {
	return DeregisterMessage{pb: pb}
}

func UnwrapDeregisterMessage(m DeregisterMessage) *public_v1.Deregister {
	return m.pb
}

func NewDeregisterMessage() DeregisterMessage {
	return WrapDeregisterMessage(&public_v1.Deregister{})
}

func (m DeregisterMessage) String() string {
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

func NewReleasedMessage(grants ...GrantID) ReleasedMessage {
	return WrapReleasedMessage(&public_v1.Released{
		Grants: slicex.Map(grants, serializeGrantID),
	})
}

func (m ReleasedMessage) Grants() []GrantID {
	return slicex.Map(m.pb.GetGrants(), deserializeGrantID)
}

func (m ReleasedMessage) String() string {
	return proto.MarshalTextString(m.pb)
}

type ExtendMessage struct {
	pb *public_v1.Extend
}

func NewExtendMessage(lease time.Time) ExtendMessage {
	return WrapExtendMessage(&public_v1.Extend{
		Lease: timestamppb.New(lease),
	})
}

func WrapExtendMessage(pb *public_v1.Extend) ExtendMessage {
	return ExtendMessage{pb: pb}
}

func UnwrapExtendMessage(m ExtendMessage) *public_v1.Extend {
	return m.pb
}

func (m ExtendMessage) GetLease() time.Time {
	return m.pb.GetLease().AsTime()
}

func (m ExtendMessage) String() string {
	return proto.MarshalTextString(m.pb)
}

type AssignMessage struct {
	pb *public_v1.Assign
}

func NewAssignMessage(grants ...Grant) AssignMessage {
	return WrapAssignMessage(&public_v1.Assign{
		Grants: slicex.Map(grants, Grant.ToProto),
	})
}

func WrapAssignMessage(pb *public_v1.Assign) AssignMessage {
	return AssignMessage{pb: pb}
}

func UnwrapAssignMessage(m AssignMessage) *public_v1.Assign {
	return m.pb
}

func (m AssignMessage) Grants() []Grant {
	ret, _ := slicex.TryMap(m.pb.GetGrants(), ParseGrant)
	return ret
}

func (m AssignMessage) String() string {
	return proto.MarshalTextString(m.pb)
}

type RevokeMessage struct {
	pb *public_v1.Revoke
}

func NewRevokeMessage(grants ...GrantID) RevokeMessage {
	return WrapRevokeMessage(&public_v1.Revoke{
		Grants: slicex.Map(grants, func(t GrantID) string {
			return string(t)
		}),
	})
}

func WrapRevokeMessage(pb *public_v1.Revoke) RevokeMessage {
	return RevokeMessage{pb: pb}
}

func UnwrapRevokeMessage(m RevokeMessage) *public_v1.Revoke {
	return m.pb
}

func (m RevokeMessage) Grants() []GrantID {
	return slicex.Map(m.pb.GetGrants(), deserializeGrantID)
}

func (m RevokeMessage) String() string {
	return proto.MarshalTextString(m.pb)
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

func NewConsumerRegisterMessage(m RegisterMessage) ConsumerMessage {
	return WrapConsumerMessage(&public_v1.ConsumerMessage{
		Msg: &public_v1.ConsumerMessage_Register{
			Register: UnwrapRegisterMessage(m),
		},
	})
}

func NewConsumerDeregisterMessage() ConsumerMessage {
	return WrapConsumerMessage(&public_v1.ConsumerMessage{
		Msg: &public_v1.ConsumerMessage_Deregister{
			Deregister: UnwrapDeregisterMessage(NewDeregisterMessage()),
		},
	})
}

func NewConsumerExtendMessage(m ExtendMessage) ConsumerMessage {
	return WrapConsumerMessage(&public_v1.ConsumerMessage{
		Msg: &public_v1.ConsumerMessage_Extend{
			Extend: UnwrapExtendMessage(m),
		},
	})
}

func NewConsumerAssignMessage(m AssignMessage) ConsumerMessage {
	return WrapConsumerMessage(&public_v1.ConsumerMessage{
		Msg: &public_v1.ConsumerMessage_Assign{
			Assign: UnwrapAssignMessage(m),
		},
	})
}

func NewConsumerRevokeMessage(m RevokeMessage) ConsumerMessage {
	return WrapConsumerMessage(&public_v1.ConsumerMessage{
		Msg: &public_v1.ConsumerMessage_Revoke{
			Revoke: UnwrapRevokeMessage(m),
		},
	})
}

func NewConsumerReleasedMessage(m ReleasedMessage) ConsumerMessage {
	return WrapConsumerMessage(&public_v1.ConsumerMessage{
		Msg: &public_v1.ConsumerMessage_Released{
			Released: UnwrapReleasedMessage(m),
		},
	})
}

func NewConsumerClusterMessage(m ClusterMessage) ConsumerMessage {
	return WrapConsumerMessage(&public_v1.ConsumerMessage{
		Msg: &public_v1.ConsumerMessage_Cluster{
			Cluster: UnwrapClusterMessage(m),
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
	return m.pb.GetDeregister() != nil
}

func (m ConsumerMessage) IsReleased() bool {
	return m.pb.GetReleased() != nil
}

func (m ConsumerMessage) Released() (ReleasedMessage, bool) {
	if !m.IsReleased() {
		return ReleasedMessage{}, false
	}
	return WrapReleasedMessage(m.pb.GetReleased()), true
}

func (m ConsumerMessage) IsExtend() bool {
	return m.pb.GetExtend() != nil
}

func (m ConsumerMessage) GetExtend() (ExtendMessage, bool) {
	if !m.IsExtend() {
		return ExtendMessage{}, false
	}
	return WrapExtendMessage(m.pb.GetExtend()), true
}

func (m ConsumerMessage) IsAssign() bool {
	return m.pb.GetAssign() != nil
}

func (m ConsumerMessage) GetAssign() (AssignMessage, bool) {
	if !m.IsAssign() {
		return AssignMessage{}, false
	}
	return WrapAssignMessage(m.pb.GetAssign()), true
}

func (m ConsumerMessage) IsRevoke() bool {
	return m.pb.GetRevoke() != nil
}

func (m ConsumerMessage) GetRevoke() (RevokeMessage, bool) {
	if !m.IsRevoke() {
		return RevokeMessage{}, false
	}
	return WrapRevokeMessage(m.pb.GetRevoke()), true
}

func (m ConsumerMessage) IsCluster() bool {
	return m.pb.GetCluster() != nil
}

func (m ConsumerMessage) GetCluster() (ClusterMessage, bool) {
	if !m.IsCluster() {
		return ClusterMessage{}, false
	}
	return WrapClusterMessage(m.pb.GetCluster()), true
}

func (m ConsumerMessage) String() string {
	return proto.MarshalTextString(m.pb)
}

type ClusterSnapshot struct {
	pb *public_v1.ClusterMessage_Snapshot
}

func WrapClusterSnapshot(pb *public_v1.ClusterMessage_Snapshot) ClusterSnapshot {
	return ClusterSnapshot{pb: pb}
}

func UnwrapClusterSnapshot(s ClusterSnapshot) *public_v1.ClusterMessage_Snapshot {
	return s.pb
}

func NewClusterSnapshot(assignments []Assignment) ClusterSnapshot {
	return ClusterSnapshot{
		pb: &public_v1.ClusterMessage_Snapshot{
			Assignments: slicex.Map(assignments, UnwrapAssignment),
		},
	}
}

func (s ClusterSnapshot) Assignments() []Assignment {
	return slicex.Map(s.pb.GetAssignments(), WrapAssignment)
}

func (s ClusterSnapshot) String() string {
	return proto.MarshalTextString(s.pb)
}

type ClusterAssign struct {
	pb *public_v1.ClusterMessage_Assign
}

func WrapClusterAssign(pb *public_v1.ClusterMessage_Assign) ClusterAssign {
	return ClusterAssign{pb: pb}
}

func UnwrapClusterAssign(a ClusterAssign) *public_v1.ClusterMessage_Assign {
	return a.pb
}

func NewClusterAssign(assignments []Assignment) ClusterAssign {
	return ClusterAssign{
		pb: &public_v1.ClusterMessage_Assign{
			Assignments: slicex.Map(assignments, UnwrapAssignment),
		},
	}
}

func (a ClusterAssign) Assignments() []Assignment {
	return slicex.Map(a.pb.GetAssignments(), WrapAssignment)
}

func (a ClusterAssign) String() string {
	return proto.MarshalTextString(a.pb)
}

type ClusterUpdate struct {
	pb *public_v1.ClusterMessage_Update
}

func WrapClusterUpdate(pb *public_v1.ClusterMessage_Update) ClusterUpdate {
	return ClusterUpdate{pb: pb}
}

func UnwrapClusterUpdate(u ClusterUpdate) *public_v1.ClusterMessage_Update {
	return u.pb
}

func NewClusterUpdate(grants []GrantInfo) ClusterUpdate {
	return ClusterUpdate{
		pb: &public_v1.ClusterMessage_Update{
			Grants: slicex.Map(grants, GrantInfo.ToProto),
		},
	}
}

func (u ClusterUpdate) Grants() ([]GrantInfo, error) {
	return slicex.TryMap(u.pb.GetGrants(), ParseGrantInfo)
}

func (u ClusterUpdate) String() string {
	return proto.MarshalTextString(u.pb)
}

type ClusterUnassign struct {
	pb *public_v1.ClusterMessage_Unassign
}

func WrapClusterUnassign(pb *public_v1.ClusterMessage_Unassign) ClusterUnassign {
	return ClusterUnassign{pb: pb}
}

func UnwrapClusterUnassign(u ClusterUnassign) *public_v1.ClusterMessage_Unassign {
	return u.pb
}

func NewClusterUnassign(grants []GrantID) ClusterUnassign {
	return ClusterUnassign{
		pb: &public_v1.ClusterMessage_Unassign{
			Grants: slicex.Map(grants, func(id GrantID) string { return string(id) }),
		},
	}
}

func (u ClusterUnassign) Grants() []GrantID {
	return slicex.Map(u.pb.GetGrants(), func(id string) GrantID { return GrantID(id) })
}

func (u ClusterUnassign) String() string {
	return proto.MarshalTextString(u.pb)
}

type ClusterDetach struct {
	pb *public_v1.ClusterMessage_Detach
}

func WrapClusterDetach(pb *public_v1.ClusterMessage_Detach) ClusterDetach {
	return ClusterDetach{pb: pb}
}

func UnwrapClusterDetach(d ClusterDetach) *public_v1.ClusterMessage_Detach {
	return d.pb
}

func NewClusterDetach(consumers []ConsumerID) ClusterDetach {
	return ClusterDetach{
		pb: &public_v1.ClusterMessage_Detach{
			Consumers: slicex.Map(consumers, func(id ConsumerID) string { return string(id) }),
		},
	}
}

func (d ClusterDetach) Consumers() []ConsumerID {
	return slicex.Map(d.pb.GetConsumers(), func(id string) ConsumerID { return ConsumerID(id) })
}

func (d ClusterDetach) String() string {
	return proto.MarshalTextString(d.pb)
}

type ClusterMessage struct {
	pb *public_v1.ClusterMessage
}

func NewClusterSnapshotMessage(snapshot ClusterSnapshot) ClusterMessage {
	return WrapClusterMessage(&public_v1.ClusterMessage{
		Msg: &public_v1.ClusterMessage_Snapshot_{
			Snapshot: UnwrapClusterSnapshot(snapshot),
		},
	})
}

func NewClusterAssignMessage(assign ClusterAssign) ClusterMessage {
	return WrapClusterMessage(&public_v1.ClusterMessage{
		Msg: &public_v1.ClusterMessage_Assign_{
			Assign: UnwrapClusterAssign(assign),
		},
	})
}

func NewClusterUpdateMessage(update ClusterUpdate) ClusterMessage {
	return WrapClusterMessage(&public_v1.ClusterMessage{
		Msg: &public_v1.ClusterMessage_Update_{
			Update: UnwrapClusterUpdate(update),
		},
	})
}

func NewClusterUnassignMessage(unassign ClusterUnassign) ClusterMessage {
	return WrapClusterMessage(&public_v1.ClusterMessage{
		Msg: &public_v1.ClusterMessage_Unassign_{
			Unassign: UnwrapClusterUnassign(unassign),
		},
	})
}

func NewClusterDetachMessage(detach ClusterDetach) ClusterMessage {
	return WrapClusterMessage(&public_v1.ClusterMessage{
		Msg: &public_v1.ClusterMessage_Detach_{
			Detach: UnwrapClusterDetach(detach),
		},
	})
}

func WrapClusterMessage(pb *public_v1.ClusterMessage) ClusterMessage {
	return ClusterMessage{pb: pb}
}

func UnwrapClusterMessage(m ClusterMessage) *public_v1.ClusterMessage {
	return m.pb
}

func (m ClusterMessage) IsSnapshot() bool {
	return m.pb.GetSnapshot() != nil
}

func (m ClusterMessage) Snapshot() (ClusterSnapshot, bool) {
	if !m.IsSnapshot() {
		return ClusterSnapshot{}, false
	}
	return WrapClusterSnapshot(m.pb.GetSnapshot()), true
}

func (m ClusterMessage) IsAssign() bool {
	return m.pb.GetAssign() != nil
}

func (m ClusterMessage) Assign() (ClusterAssign, bool) {
	if !m.IsAssign() {
		return ClusterAssign{}, false
	}
	return WrapClusterAssign(m.pb.GetAssign()), true
}

func (m ClusterMessage) IsUpdate() bool {
	return m.pb.GetUpdate() != nil
}

func (m ClusterMessage) Update() (ClusterUpdate, bool) {
	if !m.IsSnapshot() {
		return ClusterUpdate{}, false
	}
	return WrapClusterUpdate(m.pb.GetUpdate()), true
}

func (m ClusterMessage) IsUnassign() bool {
	return m.pb.GetUnassign() != nil
}

func (m ClusterMessage) Unassign() (ClusterUnassign, bool) {
	if !m.IsUnassign() {
		return ClusterUnassign{}, false
	}
	return WrapClusterUnassign(m.pb.GetUnassign()), true
}

func (m ClusterMessage) IsDetach() bool {
	return m.pb.GetDetach() != nil
}

func (m ClusterMessage) Detach() (ClusterDetach, bool) {
	if !m.IsUnassign() {
		return ClusterDetach{}, false
	}
	return WrapClusterDetach(m.pb.GetDetach()), true
}

func (m ClusterMessage) String() string {
	return proto.MarshalTextString(m.pb)
}

func serializeGrantID(g GrantID) string {
	return string(g)
}

func deserializeGrantID(str string) GrantID {
	return GrantID(str)
}
