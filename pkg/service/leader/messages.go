package leader

import (
	"go.atoms.co/splitter/lib/service/session"
	"go.atoms.co/slicex"
	"go.atoms.co/splitter/pkg/core"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pb/private"
	"go.atoms.co/splitter/pb"
	"github.com/golang/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"time"
)

type JoinMessage struct {
	pb *internal_v1.JoinMessage
}

func WrapJoinMessage(pb *internal_v1.JoinMessage) JoinMessage {
	return JoinMessage{pb: pb}
}

func UnwrapJoinMessage(m JoinMessage) *internal_v1.JoinMessage {
	return m.pb
}

func NewJoinMessage(m Message) JoinMessage {
	return JoinMessage{pb: &internal_v1.JoinMessage{
		Msg: &internal_v1.JoinMessage_Leader{
			Leader: UnwrapMessage(m),
		},
	}}
}

func NewJoinSessionMessage(m session.Message) JoinMessage {
	return JoinMessage{pb: &internal_v1.JoinMessage{
		Msg: &internal_v1.JoinMessage_Session{
			Session: session.UnwrapMessage(m),
		},
	}}
}

type Message struct {
	pb *internal_v1.LeaderMessage
}

func NewWorkerMessage(m WorkerMessage) Message {
	return Message{pb: &internal_v1.LeaderMessage{
		Msg: &internal_v1.LeaderMessage_Worker{
			Worker: UnwrapWorkerMessage(m),
		},
	}}
}

func NewClusterMessage(m ClusterMessage) Message {
	return Message{pb: &internal_v1.LeaderMessage{
		Msg: &internal_v1.LeaderMessage_Cluster{
			Cluster: UnwrapClusterMessage(m),
		},
	}}
}

func NewRegister(worker model.Instance, grants ...core.Grant) Message {
	return NewWorkerMessage(WorkerMessage{pb: &internal_v1.WorkerMessage{
		Msg: &internal_v1.WorkerMessage_Register_{
			Register: &internal_v1.WorkerMessage_Register{
				Worker: model.UnwrapInstance(worker),
				Active: slicex.Map(grants, core.UnwrapGrant),
			},
		},
	}})
}
func NewDeregister() Message {
	return NewWorkerMessage(WorkerMessage{pb: &internal_v1.WorkerMessage{
		Msg: &internal_v1.WorkerMessage_Deregister_{
			Deregister: &internal_v1.WorkerMessage_Deregister{},
		},
	}})
}

func NewDisconnect() Message {
	return NewWorkerMessage(WorkerMessage{pb: &internal_v1.WorkerMessage{
		Msg: &internal_v1.WorkerMessage_Disconnect_{
			Disconnect: &internal_v1.WorkerMessage_Disconnect{},
		},
	}})
}

func NewLeaseUpdate(ttl time.Time) Message {
	return NewWorkerMessage(WorkerMessage{pb: &internal_v1.WorkerMessage{
		Msg: &internal_v1.WorkerMessage_Lease{
			Lease: &internal_v1.WorkerMessage_LeaseUpdate{
				Ttl: timestamppb.New(ttl),
			},
		},
	}})
}

func NewAssign(grant core.Grant, state core.State) Message {
	return NewWorkerMessage(WorkerMessage{pb: &internal_v1.WorkerMessage{
		Msg: &internal_v1.WorkerMessage_Assign_{
			Assign: &internal_v1.WorkerMessage_Assign{
				Grant: core.UnwrapGrant(grant),
				State: core.UnwrapState(state),
			},
		},
	}})
}

func NewUpdate(grant core.Grant, state core.Update) Message {
	return NewWorkerMessage(WorkerMessage{pb: &internal_v1.WorkerMessage{
		Msg: &internal_v1.WorkerMessage_Update_{
			Update: &internal_v1.WorkerMessage_Update{
				Grant: core.UnwrapGrant(grant),
				State: core.UnwrapUpdate(state),
			},
		},
	}})
}

func NewRevoke(grants ...core.Grant) Message {
	return NewWorkerMessage(WorkerMessage{pb: &internal_v1.WorkerMessage{
		Msg: &internal_v1.WorkerMessage_Revoke_{
			Revoke: &internal_v1.WorkerMessage_Revoke{
				Grants: slicex.Map(grants, core.UnwrapGrant),
			},
		},
	}})
}

func NewRelinquished(grants ...core.Grant) Message {
	return NewWorkerMessage(WorkerMessage{pb: &internal_v1.WorkerMessage{
		Msg: &internal_v1.WorkerMessage_Relinquished_{
			Relinquished: &internal_v1.WorkerMessage_Relinquished{
				Grants: slicex.Map(grants, core.UnwrapGrant),
			},
		},
	}})
}

func NewClusterSnapshot(assignments []core.Assignment) Message {
	return NewClusterMessage(ClusterMessage{pb: &internal_v1.ClusterMessage{
		Msg: &internal_v1.ClusterMessage_Snapshot_{
			Snapshot: &internal_v1.ClusterMessage_Snapshot{
				Assignments: slicex.Map(assignments, core.Assignment.ToProto),
			},
		},
	}})
}

func NewClusterUpdate(assignments []core.Assignment) Message {
	return NewClusterMessage(ClusterMessage{pb: &internal_v1.ClusterMessage{
		Msg: &internal_v1.ClusterMessage_Update_{
			Update: &internal_v1.ClusterMessage_Update{
				Assignments: slicex.Map(assignments, core.Assignment.ToProto),
			},
		},
	}})
}

func NewClusterRemove(remove []model.QualifiedServiceName) Message {
	return NewClusterMessage(ClusterMessage{pb: &internal_v1.ClusterMessage{
		Msg: &internal_v1.ClusterMessage_Remove_{
			Remove: &internal_v1.ClusterMessage_Remove{
				Services: slicex.Map(remove, model.QualifiedServiceName.ToProto),
			},
		},
	}})
}

func WrapMessage(pb *internal_v1.LeaderMessage) Message {
	return Message{pb: pb}
}

func UnwrapMessage(m Message) *internal_v1.LeaderMessage {
	return m.pb
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
	return proto.MarshalTextString(m.pb)
}

type WorkerMessage struct {
	pb *internal_v1.WorkerMessage
}

func WrapWorkerMessage(pb *internal_v1.WorkerMessage) WorkerMessage {
	return WorkerMessage{pb: pb}
}

func UnwrapWorkerMessage(m WorkerMessage) *internal_v1.WorkerMessage {
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
	case m.IsDisconnect():
		return "disconnect"
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

func (m WorkerMessage) IsDisconnect() bool {
	return m.pb.GetDisconnect() != nil
}

func (m WorkerMessage) Disconnect() (DisconnectMessage, bool) {
	if !m.IsDisconnect() {
		return DisconnectMessage{}, false
	}
	return DisconnectMessage{pb: m.pb.GetDisconnect()}, true
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
	return proto.MarshalTextString(m.pb)
}

type RegisterMessage struct {
	pb *internal_v1.WorkerMessage_Register
}

func (m RegisterMessage) Worker() model.Instance {
	return model.WrapInstance(m.pb.GetWorker())
}

func (m RegisterMessage) Active() []core.Grant {
	return slicex.Map(m.pb.GetActive(), core.WrapGrant)
}

type DeregisterMessage struct {
	pb *internal_v1.WorkerMessage_Deregister
}

type LeaseUpdateMessage struct {
	pb *internal_v1.WorkerMessage_LeaseUpdate
}

func (m LeaseUpdateMessage) Ttl() time.Time {
	return m.pb.GetTtl().AsTime()
}

type DisconnectMessage struct {
	pb *internal_v1.WorkerMessage_Disconnect
}

type AssignMessage struct {
	pb *internal_v1.WorkerMessage_Assign
}

func (m AssignMessage) Grant() core.Grant {
	return core.WrapGrant(m.pb.GetGrant())
}

func (m AssignMessage) State() core.State {
	return core.WrapState(m.pb.GetState())
}

type UpdateMessage struct {
	pb *internal_v1.WorkerMessage_Update
}

func (m UpdateMessage) Grant() core.Grant {
	return core.WrapGrant(m.pb.GetGrant())
}

func (m UpdateMessage) State() core.Update {
	return core.WrapUpdate(m.pb.GetState())
}

type RevokeMessage struct {
	pb *internal_v1.WorkerMessage_Revoke
}

func (m RevokeMessage) Grants() []core.Grant {
	return slicex.Map(m.pb.GetGrants(), core.WrapGrant)
}

type RelinquishedMessage struct {
	pb *internal_v1.WorkerMessage_Relinquished
}

func (m RelinquishedMessage) Grants() []core.Grant {
	return slicex.Map(m.pb.GetGrants(), core.WrapGrant)
}

type ClusterMessage struct {
	pb *internal_v1.ClusterMessage
}

func WrapClusterMessage(pb *internal_v1.ClusterMessage) ClusterMessage {
	return ClusterMessage{pb: pb}
}

func UnwrapClusterMessage(m ClusterMessage) *internal_v1.ClusterMessage {
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

func (m ClusterMessage) String() string {
	return proto.MarshalTextString(m.pb)
}

type ClusterSnapshotMessage struct {
	pb *internal_v1.ClusterMessage_Snapshot
}

func (m ClusterSnapshotMessage) Assignments() []core.Assignment {
	return slicex.Map(m.pb.GetAssignments(), core.ParseClusterAssignment)
}

type ClusterUpdateMessage struct {
	pb *internal_v1.ClusterMessage_Update
}

func (m ClusterUpdateMessage) Assignments() []core.Assignment {
	return slicex.Map(m.pb.GetAssignments(), core.ParseClusterAssignment)
}

type ClusterRemoveMessage struct {
	pb *internal_v1.ClusterMessage_Remove
}

func (m ClusterRemoveMessage) Services() []model.QualifiedServiceName {
	return slicex.Map(m.pb.GetServices(), func(t *public_v1.QualifiedServiceName) model.QualifiedServiceName {
		ret, _ := model.ParseQualifiedServiceName(t)
		return ret
	})
}
