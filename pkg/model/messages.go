package model

import (
	"time"

	"github.com/golang/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"go.atoms.co/splitter/lib/service/location"
	"go.atoms.co/splitter/lib/service/session"
	"go.atoms.co/slicex"
	splitterpb "go.atoms.co/splitter/pb"
)

type JoinMessage struct {
	pb *splitterpb.JoinMessage
}

func WrapJoinMessage(pb *splitterpb.JoinMessage) JoinMessage {
	return JoinMessage{pb: pb}
}

func UnwrapJoinMessage(m JoinMessage) *splitterpb.JoinMessage {
	return m.pb
}

func NewJoinMessage(m ConsumerMessage) JoinMessage {
	return JoinMessage{pb: &splitterpb.JoinMessage{
		Msg: &splitterpb.JoinMessage_Consumer{
			Consumer: UnwrapConsumerMessage(m),
		},
	}}
}

func NewJoinSessionMessage(m session.Message) JoinMessage {
	return JoinMessage{pb: &splitterpb.JoinMessage{
		Msg: &splitterpb.JoinMessage_Session{
			Session: session.UnwrapMessage(m),
		},
	}}
}

type ConsumerMessage struct {
	pb *splitterpb.ConsumerMessage
}

func WrapConsumerMessage(pb *splitterpb.ConsumerMessage) ConsumerMessage {
	return ConsumerMessage{pb: pb}
}

func UnwrapConsumerMessage(m ConsumerMessage) *splitterpb.ConsumerMessage {
	return m.pb
}

func NewClientMessage(m ClientMessage) ConsumerMessage {
	return WrapConsumerMessage(&splitterpb.ConsumerMessage{
		Msg: &splitterpb.ConsumerMessage_Client{
			Client: UnwrapClientMessage(m),
		},
	})
}

func NewClusterMessage(m ClusterMessage) ConsumerMessage {
	return ConsumerMessage{pb: &splitterpb.ConsumerMessage{
		Msg: &splitterpb.ConsumerMessage_Cluster{
			Cluster: UnwrapClusterMessage(m),
		},
	}}
}

func NewRegister(consumer Consumer, service QualifiedServiceName, domains []QualifiedDomainName, grants []Grant, opts ...ConsumerOption) ConsumerMessage {
	register := &splitterpb.ClientMessage_Register{
		Consumer: UnwrapInstance(consumer),
		Service:  service.ToProto(),
		Domains:  slicex.Map(domains, QualifiedDomainName.ToProto),
		Active:   slicex.Map(grants, UnwrapGrant),
		Options:  &splitterpb.ClientMessage_Register_Options{},
	}
	for _, opt := range opts {
		opt(register.Options)
	}

	return NewClientMessage(ClientMessage{pb: &splitterpb.ClientMessage{
		Msg: &splitterpb.ClientMessage_Register_{
			Register: register,
		},
	}})
}

func NewDeregister() ConsumerMessage {
	return NewClientMessage(ClientMessage{pb: &splitterpb.ClientMessage{
		Msg: &splitterpb.ClientMessage_Deregister_{
			Deregister: &splitterpb.ClientMessage_Deregister{},
		},
	}})
}

func NewExtend(lease time.Time) ConsumerMessage {
	return NewClientMessage(ClientMessage{pb: &splitterpb.ClientMessage{
		Msg: &splitterpb.ClientMessage_Extend_{
			Extend: &splitterpb.ClientMessage_Extend{
				Lease: timestamppb.New(lease),
			},
		},
	}})
}

func NewAssign(grants ...Grant) ConsumerMessage {
	return NewClientMessage(ClientMessage{pb: &splitterpb.ClientMessage{
		Msg: &splitterpb.ClientMessage_Assign_{
			Assign: &splitterpb.ClientMessage_Assign{
				Grants: slicex.Map(grants, UnwrapGrant),
			},
		},
	}})
}

func NewPromote(grants ...Grant) ConsumerMessage {
	return NewClientMessage(ClientMessage{pb: &splitterpb.ClientMessage{
		Msg: &splitterpb.ClientMessage_Promote_{
			Promote: &splitterpb.ClientMessage_Promote{
				Grants: slicex.Map(grants, UnwrapGrant),
			},
		},
	}})
}

func NewRevoke(grants ...Grant) ConsumerMessage {
	return NewClientMessage(ClientMessage{pb: &splitterpb.ClientMessage{
		Msg: &splitterpb.ClientMessage_Revoke_{
			Revoke: &splitterpb.ClientMessage_Revoke{
				Grants: slicex.Map(grants, UnwrapGrant),
			},
		},
	}})
}

func NewReleased(grants ...Grant) ConsumerMessage {
	return NewClientMessage(ClientMessage{pb: &splitterpb.ClientMessage{
		Msg: &splitterpb.ClientMessage_Released_{
			Released: &splitterpb.ClientMessage_Released{
				Grants: slicex.Map(grants, UnwrapGrant),
			},
		},
	}})
}

func NewUpdate(grant Grant) ConsumerMessage {
	return NewClientMessage(ClientMessage{pb: &splitterpb.ClientMessage{
		Msg: &splitterpb.ClientMessage_Update_{
			Update: &splitterpb.ClientMessage_Update{
				Grant: UnwrapGrant(grant),
			},
		},
	}})
}

func NewNotify(update, target Grant) ConsumerMessage {
	return NewClientMessage(ClientMessage{pb: &splitterpb.ClientMessage{
		Msg: &splitterpb.ClientMessage_Notify_{
			Notify: &splitterpb.ClientMessage_Notify{
				Update: UnwrapGrant(update),
				Target: UnwrapGrant(target),
			},
		},
	}})
}

func NewClusterSnapshot(id ClusterID, assignments []Assignment, shards []Shard) ClusterMessage {
	return ClusterMessage{pb: &splitterpb.ClusterMessage{
		Id:        string(id.Origin.ID()),
		Version:   int64(id.Version),
		Timestamp: timestamppb.New(id.Timestamp),
		Msg: &splitterpb.ClusterMessage_Snapshot_{
			Snapshot: &splitterpb.ClusterMessage_Snapshot{
				Assignments: slicex.Map(assignments, UnwrapAssignment),
				Origin:      location.UnwrapInstance(id.Origin),
				Shards:      slicex.Map(shards, Shard.ToProto),
			},
		},
	}}
}

type ClusterChangeOption func(*splitterpb.ClusterMessage)

func WithClusterChangeShards(shards ...Shard) ClusterChangeOption {
	return func(o *splitterpb.ClusterMessage) {
		if o.GetChange() == nil {
			o.Msg = &splitterpb.ClusterMessage_Change_{Change: &splitterpb.ClusterMessage_Change{
				Shards: &splitterpb.ClusterMessage_Shards{},
			}}
		}
		if o.GetChange().GetShards() == nil {
			o.GetChange().Shards = &splitterpb.ClusterMessage_Shards{}
		}
		o.GetChange().GetShards().Shards = slicex.Map(shards, Shard.ToProto)
	}
}

func NewClusterChange(id ClusterID, assigned []Assignment, updated []GrantInfo, unassigned []GrantID, removed []ConsumerID, opts ...ClusterChangeOption) ClusterMessage {
	pb := splitterpb.ClusterMessage{
		Id:        string(id.Origin.ID()),
		Version:   int64(id.Version),
		Timestamp: timestamppb.New(id.Timestamp),
		Msg: &splitterpb.ClusterMessage_Change_{
			Change: &splitterpb.ClusterMessage_Change{
				Assign: &splitterpb.ClusterMessage_Assign{
					Assignments: slicex.Map(assigned, UnwrapAssignment),
				},
				Update: &splitterpb.ClusterMessage_Update{
					Grants: slicex.Map(updated, UnwrapGrantInfo),
				},
				Unassign: &splitterpb.ClusterMessage_Unassign{
					Grants: slicex.Map(unassigned, func(id GrantID) string { return string(id) }),
				},
				Remove: &splitterpb.ClusterMessage_Remove{
					Consumers: slicex.Map(removed, func(id ConsumerID) string { return string(id) }),
				},
			},
		},
	}
	for _, o := range opts {
		o(&pb)
	}
	return ClusterMessage{pb: &pb}
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

func (m ConsumerMessage) String() string {
	return proto.MarshalTextString(m.pb)
}

type ClientMessage struct {
	pb *splitterpb.ClientMessage
}

func WrapClientMessage(pb *splitterpb.ClientMessage) ClientMessage {
	return ClientMessage{pb: pb}
}

func UnwrapClientMessage(m ClientMessage) *splitterpb.ClientMessage {
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
	case m.IsUpdate():
		return "Update"
	case m.IsNotify():
		return "Notify"
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

func (m ClientMessage) IsUpdate() bool {
	return m.pb.GetUpdate() != nil
}

func (m ClientMessage) Update() (UpdateMessage, bool) {
	if !m.IsUpdate() {
		return UpdateMessage{}, false
	}
	return UpdateMessage{pb: m.pb.GetUpdate()}, true
}

func (m ClientMessage) IsNotify() bool {
	return m.pb.GetNotify() != nil
}

func (m ClientMessage) Notify() (NotifyMessage, bool) {
	if !m.IsNotify() {
		return NotifyMessage{}, false
	}
	return NotifyMessage{pb: m.pb.GetNotify()}, true
}

func (m ClientMessage) String() string {
	return proto.MarshalTextString(m.pb)
}

type RegisterMessage struct {
	pb *splitterpb.ClientMessage_Register
}

func WrapRegisterMessage(pb *splitterpb.ClientMessage_Register) RegisterMessage {
	return RegisterMessage{pb: pb}
}

func UnwrapRegisterMessage(m RegisterMessage) *splitterpb.ClientMessage_Register {
	return m.pb
}

func (m RegisterMessage) Consumer() Consumer {
	return WrapInstance(m.pb.GetConsumer())
}

func (m RegisterMessage) Service() QualifiedServiceName {
	ret, _ := ParseQualifiedServiceName(m.pb.GetService())
	return ret
}

func (m RegisterMessage) Domains() []QualifiedDomainName {
	domains := make([]QualifiedDomainName, len(m.pb.GetDomains()))
	for i, d := range m.pb.GetDomains() {
		domains[i], _ = ParseQualifiedDomainName(d)
	}
	return domains
}

func (m RegisterMessage) Active() []Grant {
	return slicex.Map(m.pb.GetActive(), WrapGrant)
}

type Options struct {
	pb *splitterpb.ClientMessage_Register_Options
}

func WrapOptions(pb *splitterpb.ClientMessage_Register_Options) Options {
	return Options{pb: pb}
}

func UnwrapOptions(options Options) *splitterpb.ClientMessage_Register_Options {
	return options.pb
}

func (o Options) DomainKeyNames() []DomainKeyName {
	return slicex.Map(o.pb.GetNames(), func(name *splitterpb.DomainKeyName) DomainKeyName {
		return DomainKeyName{
			Domain: DomainName(name.Domain),
			Name:   name.Name,
		}
	})
}

func (o Options) CapacityLimit() int {
	return int(o.pb.GetCapacityLimit())
}

func (m RegisterMessage) Options() Options {
	return WrapOptions(m.pb.GetOptions())
}

func (m RegisterMessage) String() string {
	return proto.MarshalTextString(m.pb)
}

type DeregisterMessage struct {
	pb *splitterpb.ClientMessage_Deregister
}

func WrapDeregisterMessage(pb *splitterpb.ClientMessage_Deregister) DeregisterMessage {
	return DeregisterMessage{pb: pb}
}

func UnwrapDeregisterMessage(m DeregisterMessage) *splitterpb.ClientMessage_Deregister {
	return m.pb
}

func (m DeregisterMessage) String() string {
	return proto.MarshalTextString(m.pb)
}

type ExtendMessage struct {
	pb *splitterpb.ClientMessage_Extend
}

func WrapExtendMessage(pb *splitterpb.ClientMessage_Extend) ExtendMessage {
	return ExtendMessage{pb: pb}
}

func UnwrapExtendMessage(m ExtendMessage) *splitterpb.ClientMessage_Extend {
	return m.pb
}

func (m ExtendMessage) Lease() time.Time {
	return m.pb.GetLease().AsTime()
}

type AssignMessage struct {
	pb *splitterpb.ClientMessage_Assign
}

func WrapAssignMessage(pb *splitterpb.ClientMessage_Assign) AssignMessage {
	return AssignMessage{pb: pb}
}

func UnwrapAssignMessage(m AssignMessage) *splitterpb.ClientMessage_Assign {
	return m.pb
}

func (m AssignMessage) Grants() []Grant {
	return slicex.Map(m.pb.GetGrants(), WrapGrant)
}

type PromoteMessage struct {
	pb *splitterpb.ClientMessage_Promote
}

func WrapPromoteMessage(pb *splitterpb.ClientMessage_Promote) PromoteMessage {
	return PromoteMessage{pb: pb}
}

func UnwrapPromoteMessage(m PromoteMessage) *splitterpb.ClientMessage_Promote {
	return m.pb
}

func (m PromoteMessage) Grants() []Grant {
	return slicex.Map(m.pb.GetGrants(), WrapGrant)
}

type RevokeMessage struct {
	pb *splitterpb.ClientMessage_Revoke
}

func WrapRevokeMessage(pb *splitterpb.ClientMessage_Revoke) RevokeMessage {
	return RevokeMessage{pb: pb}
}

func UnwrapRevokeMessage(m RevokeMessage) *splitterpb.ClientMessage_Revoke {
	return m.pb
}

func (m RevokeMessage) Grants() []Grant {
	return slicex.Map(m.pb.GetGrants(), WrapGrant)
}

type ReleasedMessage struct {
	pb *splitterpb.ClientMessage_Released
}

func WrapReleasedMessage(pb *splitterpb.ClientMessage_Released) ReleasedMessage {
	return ReleasedMessage{pb: pb}
}

func UnwrapReleasedMessage(m ReleasedMessage) *splitterpb.ClientMessage_Released {
	return m.pb
}

func (m ReleasedMessage) Grants() []Grant {
	return slicex.Map(m.pb.GetGrants(), WrapGrant)
}

func (m ReleasedMessage) String() string {
	return proto.MarshalTextString(m.pb)
}

type UpdateMessage struct {
	pb *splitterpb.ClientMessage_Update
}

func WrapUpdateMessage(pb *splitterpb.ClientMessage_Update) UpdateMessage {
	return UpdateMessage{pb: pb}
}

func UnwrapUpdateMessage(m UpdateMessage) *splitterpb.ClientMessage_Update {
	return m.pb
}

func (m UpdateMessage) Grant() Grant {
	return WrapGrant(m.pb.GetGrant())
}

func (m UpdateMessage) String() string {
	return proto.MarshalTextString(m.pb)
}

type NotifyMessage struct {
	pb *splitterpb.ClientMessage_Notify
}

func WrapNotifyMessage(pb *splitterpb.ClientMessage_Notify) NotifyMessage {
	return NotifyMessage{pb: pb}
}

func UnwrapNotifyMessage(m NotifyMessage) *splitterpb.ClientMessage_Notify {
	return m.pb
}

func (m NotifyMessage) Update() Grant {
	return WrapGrant(m.pb.GetUpdate())
}

func (m NotifyMessage) Target() Grant {
	return WrapGrant(m.pb.GetTarget())
}

func (m NotifyMessage) String() string {
	return proto.MarshalTextString(m.pb)
}

type ClusterMessage struct {
	pb *splitterpb.ClusterMessage
}

func WrapClusterMessage(pb *splitterpb.ClusterMessage) ClusterMessage {
	return ClusterMessage{pb: pb}
}

func UnwrapClusterMessage(m ClusterMessage) *splitterpb.ClusterMessage {
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

func (m ClusterMessage) ID() InstanceID {
	return InstanceID(m.pb.GetId())
}

func (m ClusterMessage) Version() int {
	return int(m.pb.GetVersion())
}

func (m ClusterMessage) Timestamp() time.Time {
	return m.pb.GetTimestamp().AsTime()
}

func (m ClusterMessage) String() string {
	return proto.MarshalTextString(m.pb)
}

type Assignment struct {
	pb *splitterpb.ClusterMessage_Assignment
}

func WrapAssignment(pb *splitterpb.ClusterMessage_Assignment) Assignment {
	return Assignment{pb: pb}
}

func UnwrapAssignment(a Assignment) *splitterpb.ClusterMessage_Assignment {
	return a.pb
}

func NewAssignment(consumer Consumer, grants ...GrantInfo) Assignment {
	return Assignment{
		pb: &splitterpb.ClusterMessage_Assignment{
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

func (a Assignment) String() string {
	return proto.MarshalTextString(a.pb)
}

type ClusterSnapshot struct {
	pb *splitterpb.ClusterMessage_Snapshot
}

func WrapClusterSnapshot(pb *splitterpb.ClusterMessage_Snapshot) ClusterSnapshot {
	return ClusterSnapshot{pb: pb}
}

func UnwrapClusterSnapshot(s ClusterSnapshot) *splitterpb.ClusterMessage_Snapshot {
	return s.pb
}

func (s ClusterSnapshot) Assignments() []Assignment {
	return slicex.Map(s.pb.GetAssignments(), WrapAssignment)
}

func (s ClusterSnapshot) Origin() (location.Instance, bool) {
	pb := s.pb.GetOrigin()
	return location.WrapInstance(pb), pb != nil
}

func (s ClusterSnapshot) Shards() ([]Shard, error) {
	return slicex.TryMap(s.pb.GetShards(), ParseShard)
}

func (s ClusterSnapshot) String() string {
	return proto.MarshalTextString(s.pb)
}

type ClusterChange struct {
	pb *splitterpb.ClusterMessage_Change
}

func WrapClusterChange(pb *splitterpb.ClusterMessage_Change) ClusterChange {
	return ClusterChange{pb: pb}
}

func UnwrapClusterChange(m ClusterChange) *splitterpb.ClusterMessage_Change {
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

func (c ClusterChange) Shards() ClusterShards {
	return WrapClusterShards(c.pb.GetShards())
}

// HasShards returns true if the change contains a new list of valid shards. The list can be empty, indicating
// that the cluster has no shards.
func (c ClusterChange) HasShards() bool {
	return c.pb.GetShards() != nil
}

func (c ClusterChange) String() string {
	return proto.MarshalTextString(c.pb)
}

type ClusterAssign struct {
	pb *splitterpb.ClusterMessage_Assign
}

func WrapClusterAssign(pb *splitterpb.ClusterMessage_Assign) ClusterAssign {
	return ClusterAssign{pb: pb}
}

func UnwrapClusterAssign(a ClusterAssign) *splitterpb.ClusterMessage_Assign {
	return a.pb
}

func (a ClusterAssign) Assignments() []Assignment {
	return slicex.Map(a.pb.GetAssignments(), WrapAssignment)
}

func (a ClusterAssign) String() string {
	return proto.MarshalTextString(a.pb)
}

type ClusterUpdate struct {
	pb *splitterpb.ClusterMessage_Update
}

func WrapClusterUpdate(pb *splitterpb.ClusterMessage_Update) ClusterUpdate {
	return ClusterUpdate{pb: pb}
}

func UnwrapClusterUpdate(u ClusterUpdate) *splitterpb.ClusterMessage_Update {
	return u.pb
}

func (u ClusterUpdate) Grants() []GrantInfo {
	return slicex.Map(u.pb.GetGrants(), WrapGrantInfo)
}

func (u ClusterUpdate) String() string {
	return proto.MarshalTextString(u.pb)
}

type ClusterUnassign struct {
	pb *splitterpb.ClusterMessage_Unassign
}

func WrapClusterUnassign(pb *splitterpb.ClusterMessage_Unassign) ClusterUnassign {
	return ClusterUnassign{pb: pb}
}

func UnwrapClusterUnassign(u ClusterUnassign) *splitterpb.ClusterMessage_Unassign {
	return u.pb
}

func (u ClusterUnassign) Grants() []GrantID {
	return slicex.Map(u.pb.GetGrants(), func(id string) GrantID { return GrantID(id) })
}

func (u ClusterUnassign) String() string {
	return proto.MarshalTextString(u.pb)
}

type ClusterRemove struct {
	pb *splitterpb.ClusterMessage_Remove
}

func WrapClusterRemove(pb *splitterpb.ClusterMessage_Remove) ClusterRemove {
	return ClusterRemove{pb: pb}
}

func UnwrapClusterRemove(r ClusterRemove) *splitterpb.ClusterMessage_Remove {
	return r.pb
}

func (d ClusterRemove) Consumers() []ConsumerID {
	return slicex.Map(d.pb.GetConsumers(), func(id string) ConsumerID { return ConsumerID(id) })
}

func (d ClusterRemove) String() string {
	return proto.MarshalTextString(d.pb)
}

type ClusterShards struct {
	pb *splitterpb.ClusterMessage_Shards
}

func WrapClusterShards(pb *splitterpb.ClusterMessage_Shards) ClusterShards {
	return ClusterShards{pb: pb}
}

func UnwrapClusterShards(s ClusterShards) *splitterpb.ClusterMessage_Shards {
	return s.pb
}

func (s ClusterShards) Shards() ([]Shard, error) {
	return slicex.TryMap(s.pb.GetShards(), ParseShard)
}
