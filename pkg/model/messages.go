package model

import (
	"go.atoms.co/splitter/lib/service/location"
	"go.atoms.co/splitter/lib/service/session"
	"go.atoms.co/slicex"
	"go.atoms.co/splitter/pb"
	"github.com/golang/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"time"
)

type JoinMessage struct {
	pb *public_v1.JoinMessage
}

func WrapJoinMessage(pb *public_v1.JoinMessage) JoinMessage {
	return JoinMessage{pb: pb}
}

func UnwrapJoinMessage(m JoinMessage) *public_v1.JoinMessage {
	return m.pb
}

func NewJoinMessage(m ConsumerMessage) JoinMessage {
	return JoinMessage{pb: &public_v1.JoinMessage{
		Msg: &public_v1.JoinMessage_Consumer{
			Consumer: UnwrapConsumerMessage(m),
		},
	}}
}

func NewJoinSessionMessage(m session.Message) JoinMessage {
	return JoinMessage{pb: &public_v1.JoinMessage{
		Msg: &public_v1.JoinMessage_Session{
			Session: session.UnwrapMessage(m),
		},
	}}
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

func NewClientMessage(m ClientMessage) ConsumerMessage {
	return WrapConsumerMessage(&public_v1.ConsumerMessage{
		Msg: &public_v1.ConsumerMessage_Client{
			Client: UnwrapClientMessage(m),
		},
	})
}

func NewClusterMessage(m ClusterMessage) ConsumerMessage {
	return ConsumerMessage{pb: &public_v1.ConsumerMessage{
		Msg: &public_v1.ConsumerMessage_Cluster{
			Cluster: UnwrapClusterMessage(m),
		},
	}}
}

func NewRegister(consumer Consumer, service QualifiedServiceName, domains []QualifiedDomainName, grants []Grant) ConsumerMessage {
	return NewClientMessage(ClientMessage{pb: &public_v1.ClientMessage{
		Msg: &public_v1.ClientMessage_Register_{
			Register: &public_v1.ClientMessage_Register{
				Consumer: UnwrapInstance(consumer),
				Service:  service.ToProto(),
				Domains:  slicex.Map(domains, QualifiedDomainName.ToProto),
				Active:   slicex.Map(grants, UnwrapGrant),
			},
		},
	}})
}
func NewDeregister() ConsumerMessage {
	return NewClientMessage(ClientMessage{pb: &public_v1.ClientMessage{
		Msg: &public_v1.ClientMessage_Deregister_{
			Deregister: &public_v1.ClientMessage_Deregister{},
		},
	}})
}

func NewExtend(lease time.Time) ConsumerMessage {
	return NewClientMessage(ClientMessage{pb: &public_v1.ClientMessage{
		Msg: &public_v1.ClientMessage_Extend_{
			Extend: &public_v1.ClientMessage_Extend{
				Lease: timestamppb.New(lease),
			},
		},
	}})
}

func NewAssign(grants ...Grant) ConsumerMessage {
	return NewClientMessage(ClientMessage{pb: &public_v1.ClientMessage{
		Msg: &public_v1.ClientMessage_Assign_{
			Assign: &public_v1.ClientMessage_Assign{
				Grants: slicex.Map(grants, UnwrapGrant),
			},
		},
	}})
}

func NewPromote(grants ...Grant) ConsumerMessage {
	return NewClientMessage(ClientMessage{pb: &public_v1.ClientMessage{
		Msg: &public_v1.ClientMessage_Promote_{
			Promote: &public_v1.ClientMessage_Promote{
				Grants: slicex.Map(grants, UnwrapGrant),
			},
		},
	}})
}

func NewRevoke(grants ...Grant) ConsumerMessage {
	return NewClientMessage(ClientMessage{pb: &public_v1.ClientMessage{
		Msg: &public_v1.ClientMessage_Revoke_{
			Revoke: &public_v1.ClientMessage_Revoke{
				Grants: slicex.Map(grants, UnwrapGrant),
			},
		},
	}})
}

func NewReleased(grants ...Grant) ConsumerMessage {
	return NewClientMessage(ClientMessage{pb: &public_v1.ClientMessage{
		Msg: &public_v1.ClientMessage_Released_{
			Released: &public_v1.ClientMessage_Released{
				Grants: slicex.Map(grants, UnwrapGrant),
			},
		},
	}})
}

func NewClusterSnapshot(id ClusterID, assignments ...Assignment) ConsumerMessage {
	return NewClusterMessage(ClusterMessage{pb: &public_v1.ClusterMessage{
		Id:        string(id.Origin.ID()),
		Version:   int64(id.Version),
		Timestamp: timestamppb.New(id.Timestamp),
		Msg: &public_v1.ClusterMessage_Snapshot_{
			Snapshot: &public_v1.ClusterMessage_Snapshot{
				Assignments: slicex.Map(assignments, UnwrapAssignment),
				Origin:      location.UnwrapInstance(id.Origin),
			},
		},
	}})
}

func NewClusterChange(id ClusterID, assigned []Assignment, updated []GrantInfo, unassigned []GrantID, removed []ConsumerID) ConsumerMessage {
	return NewClusterMessage(ClusterMessage{pb: &public_v1.ClusterMessage{
		Id:        string(id.Origin.ID()),
		Version:   int64(id.Version),
		Timestamp: timestamppb.New(id.Timestamp),
		Msg: &public_v1.ClusterMessage_Change_{
			Change: &public_v1.ClusterMessage_Change{
				Assign: &public_v1.ClusterMessage_Assign{
					Assignments: slicex.Map(assigned, UnwrapAssignment),
				},
				Update: &public_v1.ClusterMessage_Update{
					Grants: slicex.Map(updated, UnwrapGrantInfo),
				},
				Unassign: &public_v1.ClusterMessage_Unassign{
					Grants: slicex.Map(unassigned, func(id GrantID) string { return string(id) }),
				},
				Remove: &public_v1.ClusterMessage_Remove{
					Consumers: slicex.Map(removed, func(id ConsumerID) string { return string(id) }),
				},
			},
		},
	}})
}

func (m ConsumerMessage) Type() string {
	switch {
	case m.IsClientMessage():
		msg, _ := m.ClientMessage()
		return "client/" + msg.Type()
	case m.IsClusterMessage():
		msg, _ := m.ClusterMessage()
		return "cluster/" + msg.Type()
	default:
		return "unknown"
	}
}

func (m ConsumerMessage) IsClientMessage() bool {
	return m.pb.GetClient() != nil
}

func (m ConsumerMessage) ClientMessage() (ClientMessage, bool) {
	return WrapClientMessage(m.pb.GetClient()), m.pb.GetClient() != nil
}

func (m ConsumerMessage) IsClusterMessage() bool {
	return m.pb.GetCluster() != nil
}

func (m ConsumerMessage) ClusterMessage() (ClusterMessage, bool) {
	if !m.IsClusterMessage() {
		return ClusterMessage{}, false
	}
	return WrapClusterMessage(m.pb.GetCluster()), true
}

func (m ClusterMessage) ID() InstanceID {
	return InstanceID(m.pb.GetId())
}

func (m ClusterMessage) Version() int {
	return int(m.pb.GetVersion())
}

func (m ClusterMessage) Timestamp() time.Time {
	return m.pb.GetTimestamp().AsTime()
}

func (m ConsumerMessage) String() string {
	return proto.MarshalTextString(m.pb)
}

type ClientMessage struct {
	pb *public_v1.ClientMessage
}

func WrapClientMessage(pb *public_v1.ClientMessage) ClientMessage {
	return ClientMessage{pb: pb}
}

func UnwrapClientMessage(m ClientMessage) *public_v1.ClientMessage {
	return m.pb
}

func (m ClientMessage) Type() string {
	switch {
	case m.IsRegister():
		return "register"
	case m.IsDeregister():
		return "deregister"
	case m.IsExtend():
		return "extend"
	case m.IsAssign():
		return "assign"
	case m.IsPromote():
		return "promote"
	case m.IsRevoke():
		return "revoke"
	case m.IsReleased():
		return "Released"
	default:
		return "unknown"
	}
}

func (m ClientMessage) IsRegister() bool {
	return m.pb.GetRegister() != nil
}

func (m ClientMessage) Register() (RegisterMessage, bool) {
	if !m.IsRegister() {
		return RegisterMessage{}, false
	}
	return RegisterMessage{pb: m.pb.GetRegister()}, true
}

func (m ClientMessage) IsDeregister() bool {
	return m.pb.GetDeregister() != nil
}

func (m ClientMessage) Deregister() (DeregisterMessage, bool) {
	if !m.IsDeregister() {
		return DeregisterMessage{}, false
	}
	return DeregisterMessage{pb: m.pb.GetDeregister()}, true
}

func (m ClientMessage) IsExtend() bool {
	return m.pb.GetExtend() != nil
}

func (m ClientMessage) Extend() (ExtendMessage, bool) {
	if !m.IsExtend() {
		return ExtendMessage{}, false
	}
	return ExtendMessage{pb: m.pb.GetExtend()}, true
}

func (m ClientMessage) IsAssign() bool {
	return m.pb.GetAssign() != nil
}

func (m ClientMessage) Assign() (AssignMessage, bool) {
	if !m.IsAssign() {
		return AssignMessage{}, false
	}
	return AssignMessage{pb: m.pb.GetAssign()}, true
}

func (m ClientMessage) IsPromote() bool {
	return m.pb.GetPromote() != nil
}

func (m ClientMessage) Promote() (PromoteMessage, bool) {
	if !m.IsPromote() {
		return PromoteMessage{}, false
	}
	return PromoteMessage{pb: m.pb.GetPromote()}, true
}

func (m ClientMessage) IsRevoke() bool {
	return m.pb.GetRevoke() != nil
}

func (m ClientMessage) Revoke() (RevokeMessage, bool) {
	if !m.IsRevoke() {
		return RevokeMessage{}, false
	}
	return RevokeMessage{pb: m.pb.GetRevoke()}, true
}

func (m ClientMessage) IsReleased() bool {
	return m.pb.GetReleased() != nil
}

func (m ClientMessage) Released() (ReleasedMessage, bool) {
	if !m.IsReleased() {
		return ReleasedMessage{}, false
	}
	return ReleasedMessage{pb: m.pb.GetReleased()}, true
}

func (m ClientMessage) String() string {
	return proto.MarshalTextString(m.pb)
}

type RegisterMessage struct {
	pb *public_v1.ClientMessage_Register
}

func WrapRegisterMessage(pb *public_v1.ClientMessage_Register) RegisterMessage {
	return RegisterMessage{pb: pb}
}

func UnwrapRegisterMessage(m RegisterMessage) *public_v1.ClientMessage_Register {
	return m.pb
}

func (m RegisterMessage) Service() QualifiedServiceName {
	ret, _ := ParseQualifiedServiceName(m.pb.GetService())
	return ret
}

func (m RegisterMessage) Consumer() Consumer {
	return WrapInstance(m.pb.GetConsumer())
}

func (m RegisterMessage) Active() []Grant {
	return slicex.Map(m.pb.GetActive(), WrapGrant)
}

func (m RegisterMessage) String() string {
	return proto.MarshalTextString(m.pb)
}

type DeregisterMessage struct {
	pb *public_v1.ClientMessage_Deregister
}

func WrapDeregisterMessage(pb *public_v1.ClientMessage_Deregister) DeregisterMessage {
	return DeregisterMessage{pb: pb}
}

func UnwrapDeregisterMessage(m DeregisterMessage) *public_v1.ClientMessage_Deregister {
	return m.pb
}

func (m DeregisterMessage) String() string {
	return proto.MarshalTextString(m.pb)
}

type ExtendMessage struct {
	pb *public_v1.ClientMessage_Extend
}

func WrapExtendMessage(pb *public_v1.ClientMessage_Extend) ExtendMessage {
	return ExtendMessage{pb: pb}
}

func UnwrapExtendMessage(m ExtendMessage) *public_v1.ClientMessage_Extend {
	return m.pb
}

func (m ExtendMessage) Lease() time.Time {
	return m.pb.GetLease().AsTime()
}

type AssignMessage struct {
	pb *public_v1.ClientMessage_Assign
}

func WrapAssignMessage(pb *public_v1.ClientMessage_Assign) AssignMessage {
	return AssignMessage{pb: pb}
}

func UnwrapAssignMessage(m AssignMessage) *public_v1.ClientMessage_Assign {
	return m.pb
}

func (m AssignMessage) Grants() []Grant {
	return slicex.Map(m.pb.GetGrants(), WrapGrant)
}

type PromoteMessage struct {
	pb *public_v1.ClientMessage_Promote
}

func WrapPromoteMessage(pb *public_v1.ClientMessage_Promote) PromoteMessage {
	return PromoteMessage{pb: pb}
}

func UnwrapPromoteMessage(m PromoteMessage) *public_v1.ClientMessage_Promote {
	return m.pb
}

func (m PromoteMessage) Grants() []Grant {
	return slicex.Map(m.pb.GetGrants(), WrapGrant)
}

type RevokeMessage struct {
	pb *public_v1.ClientMessage_Revoke
}

func WrapRevokeMessage(pb *public_v1.ClientMessage_Revoke) RevokeMessage {
	return RevokeMessage{pb: pb}
}

func UnwrapRevokeMessage(m RevokeMessage) *public_v1.ClientMessage_Revoke {
	return m.pb
}

func (m RevokeMessage) Grants() []Grant {
	return slicex.Map(m.pb.GetGrants(), WrapGrant)
}

type ReleasedMessage struct {
	pb *public_v1.ClientMessage_Released
}

func WrapReleasedMessage(pb *public_v1.ClientMessage_Released) ReleasedMessage {
	return ReleasedMessage{pb: pb}
}

func UnwrapReleasedMessage(m ReleasedMessage) *public_v1.ClientMessage_Released {
	return m.pb
}

func (m ReleasedMessage) Grants() []Grant {
	return slicex.Map(m.pb.GetGrants(), WrapGrant)
}

func (m ReleasedMessage) String() string {
	return proto.MarshalTextString(m.pb)
}

type ClusterMessage struct {
	pb *public_v1.ClusterMessage
}

func WrapClusterMessage(pb *public_v1.ClusterMessage) ClusterMessage {
	return ClusterMessage{pb: pb}
}

func UnwrapClusterMessage(m ClusterMessage) *public_v1.ClusterMessage {
	return m.pb
}

func (m ClusterMessage) Type() string {
	switch {
	case m.IsSnapshot():
		return "snapshot"
	case m.IsChange():
		return "change"
	default:
		return "unknown"
	}
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

func (m ClusterMessage) IsChange() bool {
	return m.pb.GetChange() != nil
}

func (m ClusterMessage) Change() (ClusterChange, bool) {
	if !m.IsChange() {
		return ClusterChange{}, false
	}
	return WrapClusterChange(m.pb.GetChange()), true
}

func (m ClusterMessage) String() string {
	return proto.MarshalTextString(m.pb)
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
			Grants:   slicex.Map(grants, UnwrapGrantInfo),
		},
	}
}

func (a Assignment) Consumer() Consumer {
	return WrapInstance(a.pb.GetConsumer())
}

func (a Assignment) Grants() []GrantInfo {
	return slicex.Map(a.pb.GetGrants(), WrapGrantInfo)
}

func ClusterToAssignments(c Cluster) []Assignment {
	return slicex.Map(c.Consumers(), func(consumer Consumer) Assignment {
		_, grants, _ := c.Consumer(consumer.ID())
		return NewAssignment(consumer, grants...)
	})
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

func (s ClusterSnapshot) Assignments() []Assignment {
	return slicex.Map(s.pb.GetAssignments(), WrapAssignment)
}

func (s ClusterSnapshot) Origin() (location.Instance, bool) {
	pb := s.pb.GetOrigin()
	return location.WrapInstance(pb), pb != nil
}

func (s ClusterSnapshot) String() string {
	return proto.MarshalTextString(s.pb)
}

type ClusterChange struct {
	pb *public_v1.ClusterMessage_Change
}

func WrapClusterChange(pb *public_v1.ClusterMessage_Change) ClusterChange {
	return ClusterChange{pb: pb}
}

func UnwrapClusterChange(m ClusterChange) *public_v1.ClusterMessage_Change {
	return m.pb
}

func (c ClusterChange) Assign() ClusterAssign {
	return WrapClusterAssign(c.pb.GetAssign())
}

func (c ClusterChange) Update() ClusterUpdate {
	return WrapClusterUpdate(c.pb.GetUpdate())
}

func (c ClusterChange) Unassign() ClusterUnassign {
	return WrapClusterUnassign(c.pb.GetUnassign())
}

func (c ClusterChange) Remove() ClusterRemove {
	return WrapClusterRemove(c.pb.GetRemove())
}

func (c ClusterChange) String() string {
	return proto.MarshalTextString(c.pb)
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

func (u ClusterUpdate) Grants() []GrantInfo {
	return slicex.Map(u.pb.GetGrants(), WrapGrantInfo)
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

func (u ClusterUnassign) Grants() []GrantID {
	return slicex.Map(u.pb.GetGrants(), func(id string) GrantID { return GrantID(id) })
}

func (u ClusterUnassign) String() string {
	return proto.MarshalTextString(u.pb)
}

type ClusterRemove struct {
	pb *public_v1.ClusterMessage_Remove
}

func WrapClusterRemove(pb *public_v1.ClusterMessage_Remove) ClusterRemove {
	return ClusterRemove{pb: pb}
}

func UnwrapClusterRemove(d ClusterRemove) *public_v1.ClusterMessage_Remove {
	return d.pb
}

func (d ClusterRemove) Consumers() []ConsumerID {
	return slicex.Map(d.pb.GetConsumers(), func(id string) ConsumerID { return ConsumerID(id) })
}

func (d ClusterRemove) String() string {
	return proto.MarshalTextString(d.pb)
}
