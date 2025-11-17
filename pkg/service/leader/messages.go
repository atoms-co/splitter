package leader

import (
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"go.atoms.co/splitter/lib/service/location"
	"go.atoms.co/splitter/lib/service/session"
	"go.atoms.co/lib/encoding/protox"
	"go.atoms.co/slicex"
	"go.atoms.co/splitter/pkg/core"
	"go.atoms.co/splitter/pkg/model"
	splitterprivatepb "go.atoms.co/splitter/pb/private"
	splitterpb "go.atoms.co/splitter/pb"
)

type JoinMessage struct {
	pb *splitterprivatepb.JoinMessage
}

func WrapJoinMessage(pb *splitterprivatepb.JoinMessage) JoinMessage {
	return JoinMessage{pb: pb}
}

func UnwrapJoinMessage(m JoinMessage) *splitterprivatepb.JoinMessage {
	return m.pb
}

func NewJoinMessage(m Message) JoinMessage {
	return JoinMessage{pb: &splitterprivatepb.JoinMessage{
		Msg: &splitterprivatepb.JoinMessage_Leader{
			Leader: UnwrapMessage(m),
		},
	}}
}

func NewJoinSessionMessage(m session.Message) JoinMessage {
	return JoinMessage{pb: &splitterprivatepb.JoinMessage{
		Msg: &splitterprivatepb.JoinMessage_Session{
			Session: session.UnwrapMessage(m),
		},
	}}
}

type Message struct {
	pb *splitterprivatepb.LeaderMessage
}

func WrapMessage(pb *splitterprivatepb.LeaderMessage) Message {
	return Message{pb: pb}
}

func UnwrapMessage(m Message) *splitterprivatepb.LeaderMessage {
	return m.pb
}

func NewWorkerMessage(m WorkerMessage) Message {
	return Message{pb: &splitterprivatepb.LeaderMessage{
		Msg: &splitterprivatepb.LeaderMessage_Worker{
			Worker: UnwrapWorkerMessage(m),
		},
	}}
}

func NewClusterMessage(m ClusterMessage) Message {
	return Message{pb: &splitterprivatepb.LeaderMessage{
		Msg: &splitterprivatepb.LeaderMessage_Cluster{
			Cluster: UnwrapClusterMessage(m),
		},
	}}
}

func NewRegister(worker model.Instance, grants ...core.Grant) Message {
	return NewWorkerMessage(WorkerMessage{pb: &splitterprivatepb.WorkerMessage{
		Msg: &splitterprivatepb.WorkerMessage_Register_{
			Register: &splitterprivatepb.WorkerMessage_Register{
				Worker: model.UnwrapInstance(worker),
				Active: slicex.Map(grants, core.UnwrapGrant),
			},
		},
	}})
}
func NewDeregister() Message {
	return NewWorkerMessage(WorkerMessage{pb: &splitterprivatepb.WorkerMessage{
		Msg: &splitterprivatepb.WorkerMessage_Deregister_{
			Deregister: &splitterprivatepb.WorkerMessage_Deregister{},
		},
	}})
}
func NewLeaseUpdate(ttl time.Time) Message {
	return NewWorkerMessage(WorkerMessage{pb: &splitterprivatepb.WorkerMessage{
		Msg: &splitterprivatepb.WorkerMessage_Lease{
			Lease: &splitterprivatepb.WorkerMessage_LeaseUpdate{
				Ttl: timestamppb.New(ttl),
			},
		},
	}})
}

func NewAssign(grant core.Grant, state core.State) Message {
	return NewWorkerMessage(WorkerMessage{pb: &splitterprivatepb.WorkerMessage{
		Msg: &splitterprivatepb.WorkerMessage_Assign_{
			Assign: &splitterprivatepb.WorkerMessage_Assign{
				Grant: core.UnwrapGrant(grant),
				State: core.UnwrapState(state),
			},
		},
	}})
}

func NewUpdate(grant core.Grant, state core.Update) Message {
	return NewWorkerMessage(WorkerMessage{pb: &splitterprivatepb.WorkerMessage{
		Msg: &splitterprivatepb.WorkerMessage_Update_{
			Update: &splitterprivatepb.WorkerMessage_Update{
				Grant: core.UnwrapGrant(grant),
				State: core.UnwrapUpdate(state),
			},
		},
	}})
}

func NewRevoke(grants ...core.Grant) Message {
	return NewWorkerMessage(WorkerMessage{pb: &splitterprivatepb.WorkerMessage{
		Msg: &splitterprivatepb.WorkerMessage_Revoke_{
			Revoke: &splitterprivatepb.WorkerMessage_Revoke{
				Grants: slicex.Map(grants, core.UnwrapGrant),
			},
		},
	}})
}

func NewRelinquished(grants ...core.Grant) Message {
	return NewWorkerMessage(WorkerMessage{pb: &splitterprivatepb.WorkerMessage{
		Msg: &splitterprivatepb.WorkerMessage_Relinquished_{
			Relinquished: &splitterprivatepb.WorkerMessage_Relinquished{
				Grants: slicex.Map(grants, core.UnwrapGrant),
			},
		},
	}})
}

func NewClusterSnapshot(id model.ClusterID, assignments []core.Assignment) Message {
	return NewClusterMessage(ClusterMessage{pb: &splitterprivatepb.ClusterMessage{
		Id:        string(id.Origin.ID()),
		Version:   int64(id.Version),
		Timestamp: timestamppb.New(id.Timestamp),
		Msg: &splitterprivatepb.ClusterMessage_Snapshot_{
			Snapshot: &splitterprivatepb.ClusterMessage_Snapshot{
				Assignments: slicex.Map(assignments, core.Assignment.ToProto),
				Origin:      location.UnwrapInstance(id.Origin),
			},
		},
	}})
}

func NewClusterUpdate(id model.ClusterID, assignments []core.Assignment) Message {
	return NewClusterMessage(ClusterMessage{pb: &splitterprivatepb.ClusterMessage{
		Id:        string(id.Origin.ID()),
		Version:   int64(id.Version),
		Timestamp: timestamppb.New(id.Timestamp),
		Msg: &splitterprivatepb.ClusterMessage_Update_{
			Update: &splitterprivatepb.ClusterMessage_Update{
				Assignments: slicex.Map(assignments, core.Assignment.ToProto),
			},
		},
	}})
}

func NewClusterRemove(id model.ClusterID, remove []model.QualifiedServiceName) Message {
	return NewClusterMessage(ClusterMessage{pb: &splitterprivatepb.ClusterMessage{
		Id:        string(id.Origin.ID()),
		Version:   int64(id.Version),
		Timestamp: timestamppb.New(id.Timestamp),
		Msg: &splitterprivatepb.ClusterMessage_Remove_{
			Remove: &splitterprivatepb.ClusterMessage_Remove{
				Services: slicex.Map(remove, model.QualifiedServiceName.ToProto),
			},
		},
	}})
}

func (m Message) Type() string {
	switch {
	case m.IsWorkerMessage():
		msg, _ := m.WorkerMessage()
		return "worker/" + msg.Type()
	case m.IsClusterMessage():
		msg, _ := m.ClusterMessage()
		return "cluster/" + msg.Type()
	default:
		return "unknown"
	}
}

func (m Message) IsWorkerMessage() bool {
	return m.pb.GetWorker() != nil
}

func (m Message) WorkerMessage() (WorkerMessage, bool) {
	return WrapWorkerMessage(m.pb.GetWorker()), m.pb.GetWorker() != nil
}

func (m Message) IsClusterMessage() bool {
	return m.pb.GetCluster() != nil
}

func (m Message) ClusterMessage() (ClusterMessage, bool) {
	return WrapClusterMessage(m.pb.GetCluster()), m.pb.GetCluster() != nil
}

func (m Message) String() string {
	return protox.MarshalTextString(m.pb)
}

type WorkerMessage struct {
	pb *splitterprivatepb.WorkerMessage
}

func WrapWorkerMessage(pb *splitterprivatepb.WorkerMessage) WorkerMessage {
	return WorkerMessage{pb: pb}
}

func UnwrapWorkerMessage(m WorkerMessage) *splitterprivatepb.WorkerMessage {
	return m.pb
}

func (m WorkerMessage) Type() string {
	switch {
	case m.IsRegister():
		return "register"
	case m.IsDeregister():
		return "deregister"
	case m.IsLeaseUpdate():
		return "lease"
	case m.IsAssign():
		return "assign"
	case m.IsUpdate():
		return "update"
	case m.IsRevoke():
		return "revoke"
	case m.IsRelinquished():
		return "relinquished"
	default:
		return "unknown"
	}
}

func (m WorkerMessage) IsRegister() bool {
	return m.pb.GetRegister() != nil
}

func (m WorkerMessage) Register() (RegisterMessage, bool) {
	if !m.IsRegister() {
		return RegisterMessage{}, false
	}
	return RegisterMessage{pb: m.pb.GetRegister()}, true
}

func (m WorkerMessage) IsDeregister() bool {
	return m.pb.GetDeregister() != nil
}

func (m WorkerMessage) Deregister() (DeregisterMessage, bool) {
	if !m.IsDeregister() {
		return DeregisterMessage{}, false
	}
	return DeregisterMessage{pb: m.pb.GetDeregister()}, true
}

func (m WorkerMessage) IsLeaseUpdate() bool {
	return m.pb.GetLease() != nil
}

func (m WorkerMessage) LeaseUpdate() (LeaseUpdateMessage, bool) {
	if !m.IsLeaseUpdate() {
		return LeaseUpdateMessage{}, false
	}
	return LeaseUpdateMessage{pb: m.pb.GetLease()}, true
}

func (m WorkerMessage) IsAssign() bool {
	return m.pb.GetAssign() != nil
}

func (m WorkerMessage) Assign() (AssignMessage, bool) {
	if !m.IsAssign() {
		return AssignMessage{}, false
	}
	return AssignMessage{pb: m.pb.GetAssign()}, true
}

func (m WorkerMessage) IsUpdate() bool {
	return m.pb.GetUpdate() != nil
}

func (m WorkerMessage) Update() (UpdateMessage, bool) {
	if !m.IsUpdate() {
		return UpdateMessage{}, false
	}
	return UpdateMessage{pb: m.pb.GetUpdate()}, true
}
func (m WorkerMessage) IsRevoke() bool {
	return m.pb.GetRevoke() != nil
}

func (m WorkerMessage) Revoke() (RevokeMessage, bool) {
	if !m.IsRevoke() {
		return RevokeMessage{}, false
	}
	return RevokeMessage{pb: m.pb.GetRevoke()}, true
}

func (m WorkerMessage) IsRelinquished() bool {
	return m.pb.GetRelinquished() != nil
}

func (m WorkerMessage) Relinquished() (RelinquishedMessage, bool) {
	if !m.IsRelinquished() {
		return RelinquishedMessage{}, false
	}
	return RelinquishedMessage{pb: m.pb.GetRelinquished()}, true
}

func (m WorkerMessage) String() string {
	return protox.MarshalTextString(m.pb)
}

type RegisterMessage struct {
	pb *splitterprivatepb.WorkerMessage_Register
}

func (m RegisterMessage) Worker() model.Instance {
	return model.WrapInstance(m.pb.GetWorker())
}

func (m RegisterMessage) Active() []core.Grant {
	return slicex.Map(m.pb.GetActive(), core.WrapGrant)
}

func (m RegisterMessage) String() string {
	return protox.MarshalTextString(m.pb)
}

type DeregisterMessage struct {
	pb *splitterprivatepb.WorkerMessage_Deregister
}

func (m DeregisterMessage) String() string {
	return protox.MarshalTextString(m.pb)
}

type LeaseUpdateMessage struct {
	pb *splitterprivatepb.WorkerMessage_LeaseUpdate
}

func (m LeaseUpdateMessage) Ttl() time.Time {
	return m.pb.GetTtl().AsTime()
}

func (m LeaseUpdateMessage) String() string {
	return protox.MarshalTextString(m.pb)
}

type AssignMessage struct {
	pb *splitterprivatepb.WorkerMessage_Assign
}

func (m AssignMessage) Grant() core.Grant {
	return core.WrapGrant(m.pb.GetGrant())
}

func (m AssignMessage) State() core.State {
	return core.WrapState(m.pb.GetState())
}

func (m AssignMessage) String() string {
	return protox.MarshalTextString(m.pb)
}

type UpdateMessage struct {
	pb *splitterprivatepb.WorkerMessage_Update
}

func (m UpdateMessage) Grant() core.Grant {
	return core.WrapGrant(m.pb.GetGrant())
}

func (m UpdateMessage) State() core.Update {
	return core.WrapUpdate(m.pb.GetState())
}

func (m UpdateMessage) String() string {
	return protox.MarshalTextString(m.pb)
}

type RevokeMessage struct {
	pb *splitterprivatepb.WorkerMessage_Revoke
}

func (m RevokeMessage) Grants() []core.Grant {
	return slicex.Map(m.pb.GetGrants(), core.WrapGrant)
}

func (m RevokeMessage) String() string {
	return protox.MarshalTextString(m.pb)
}

type RelinquishedMessage struct {
	pb *splitterprivatepb.WorkerMessage_Relinquished
}

func (m RelinquishedMessage) Grants() []core.Grant {
	return slicex.Map(m.pb.GetGrants(), core.WrapGrant)
}

func (m RelinquishedMessage) String() string {
	return protox.MarshalTextString(m.pb)
}

type ClusterMessage struct {
	pb *splitterprivatepb.ClusterMessage
}

func WrapClusterMessage(pb *splitterprivatepb.ClusterMessage) ClusterMessage {
	return ClusterMessage{pb: pb}
}

func UnwrapClusterMessage(m ClusterMessage) *splitterprivatepb.ClusterMessage {
	return m.pb
}

func (m ClusterMessage) Type() string {
	switch {
	case m.IsSnapshot():
		return "snapshot"
	case m.IsUpdate():
		return "update"
	case m.IsRemove():
		return "remove"
	default:
		return "unknown"
	}
}

func (m ClusterMessage) IsSnapshot() bool {
	return m.pb.GetSnapshot() != nil
}

func (m ClusterMessage) IsUpdate() bool {
	return m.pb.GetUpdate() != nil
}

func (m ClusterMessage) IsRemove() bool {
	return m.pb.GetRemove() != nil
}

func (m ClusterMessage) Snapshot() (ClusterSnapshotMessage, bool) {
	if !m.IsSnapshot() {
		return ClusterSnapshotMessage{}, false
	}
	return ClusterSnapshotMessage{pb: m.pb.GetSnapshot()}, true
}

func (m ClusterMessage) Update() (ClusterUpdateMessage, bool) {
	if !m.IsUpdate() {
		return ClusterUpdateMessage{}, false
	}
	return ClusterUpdateMessage{pb: m.pb.GetUpdate()}, true
}

func (m ClusterMessage) Remove() (ClusterRemoveMessage, bool) {
	if !m.IsRemove() {
		return ClusterRemoveMessage{}, false
	}
	return ClusterRemoveMessage{pb: m.pb.GetRemove()}, true
}

func (m ClusterMessage) ID() model.InstanceID {
	return model.InstanceID(m.pb.GetId())
}

func (m ClusterMessage) Version() int {
	return int(m.pb.GetVersion())
}

func (m ClusterMessage) Timestamp() time.Time {
	return m.pb.GetTimestamp().AsTime()
}

func (m ClusterMessage) String() string {
	return protox.MarshalTextString(m.pb)
}

type ClusterSnapshotMessage struct {
	pb *splitterprivatepb.ClusterMessage_Snapshot
}

func (m ClusterSnapshotMessage) Assignments() []core.Assignment {
	return slicex.Map(m.pb.GetAssignments(), core.ParseClusterAssignment)
}

func (m ClusterSnapshotMessage) Origin() (location.Instance, bool) {
	pb := m.pb.GetOrigin()
	return location.WrapInstance(pb), pb != nil
}

type ClusterUpdateMessage struct {
	pb *splitterprivatepb.ClusterMessage_Update
}

func (m ClusterUpdateMessage) Assignments() []core.Assignment {
	return slicex.Map(m.pb.GetAssignments(), core.ParseClusterAssignment)
}

type ClusterRemoveMessage struct {
	pb *splitterprivatepb.ClusterMessage_Remove
}

func (m ClusterRemoveMessage) Services() []model.QualifiedServiceName {
	return slicex.Map(m.pb.GetServices(), func(t *splitterpb.QualifiedServiceName) model.QualifiedServiceName {
		ret, _ := model.ParseQualifiedServiceName(t)
		return ret
	})
}
