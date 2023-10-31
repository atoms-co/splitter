package leader

import (
	"go.atoms.co/slicex"
	"go.atoms.co/splitter/pkg/core"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pb/private"
	"github.com/golang/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"time"
)

type JoinMessage struct {
	pb *internal_v1.JoinMessage
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
func (m Message) IsWorkerMessage() bool {
	return m.pb.GetWorker() != nil
}

func (m Message) WorkerMessage() (WorkerMessage, bool) {
	return WrapWorkerMessage(m.pb.GetWorker()), m.pb.GetWorker() != nil
}
func (m Message) String() string {
	return proto.MarshalTextString(m.pb)
}

type WorkerMessage struct {
	pb *internal_v1.WorkerMessage
}

func NewRegister(worker model.Instance, grants ...core.Grant) WorkerMessage {
	return WorkerMessage{pb: &internal_v1.WorkerMessage{
		Msg: &internal_v1.WorkerMessage_Register_{
			Register: &internal_v1.WorkerMessage_Register{
				Worker: model.UnwrapInstance(worker),
				Active: slicex.Map(grants, core.UnwrapGrant),
			},
		},
	}}
}
func NewDeregister() WorkerMessage {
	return WorkerMessage{pb: &internal_v1.WorkerMessage{
		Msg: &internal_v1.WorkerMessage_Deregister_{
			Deregister: &internal_v1.WorkerMessage_Deregister{},
		},
	}}
}

func NewDisconnect() WorkerMessage {
	return WorkerMessage{pb: &internal_v1.WorkerMessage{
		Msg: &internal_v1.WorkerMessage_Disconnect_{
			Disconnect: &internal_v1.WorkerMessage_Disconnect{},
		},
	}}
}
func NewLeaseUpdate(ttl time.Time) WorkerMessage {
	return WorkerMessage{pb: &internal_v1.WorkerMessage{
		Msg: &internal_v1.WorkerMessage_Lease{
			Lease: &internal_v1.WorkerMessage_LeaseUpdate{
				Ttl: timestamppb.New(ttl),
			},
		},
	}}
}

func NewAssign(grant core.Grant, state core.State) WorkerMessage {
	return WorkerMessage{pb: &internal_v1.WorkerMessage{
		Msg: &internal_v1.WorkerMessage_Assign_{
			Assign: &internal_v1.WorkerMessage_Assign{
				Grant: core.UnwrapGrant(grant),
				State: core.UnwrapState(state),
			},
		},
	}}
}

func NewRevoke(grants ...core.Grant) WorkerMessage {
	return WorkerMessage{pb: &internal_v1.WorkerMessage{
		Msg: &internal_v1.WorkerMessage_Revoke_{
			Revoke: &internal_v1.WorkerMessage_Revoke{
				Grants: slicex.Map(grants, core.UnwrapGrant),
			},
		},
	}}
}

func NewRelinquished(grants ...core.Grant) WorkerMessage {
	return WorkerMessage{pb: &internal_v1.WorkerMessage{
		Msg: &internal_v1.WorkerMessage_Relinquished_{
			Relinquished: &internal_v1.WorkerMessage_Relinquished{
				Grants: slicex.Map(grants, core.UnwrapGrant),
			},
		},
	}}
}

func WrapWorkerMessage(pb *internal_v1.WorkerMessage) WorkerMessage {
	return WorkerMessage{pb: pb}
}

func UnwrapWorkerMessage(m WorkerMessage) *internal_v1.WorkerMessage {
	return m.pb
}

func (m *WorkerMessage) IsRegister() bool {
	return m.pb.GetRegister() != nil
}

func (m *WorkerMessage) Register() (RegisterMessage, bool) {
	if !m.IsRegister() {
		return RegisterMessage{}, false
	}
	return RegisterMessage{pb: m.pb.GetRegister()}, true
}

func (m *WorkerMessage) IsDeregister() bool {
	return m.pb.GetDeregister() != nil
}

func (m *WorkerMessage) Deregister() (DeregisterMessage, bool) {
	if !m.IsDeregister() {
		return DeregisterMessage{}, false
	}
	return DeregisterMessage{pb: m.pb.GetDeregister()}, true
}

func (m *WorkerMessage) IsLeaseUpdate() bool {
	return m.pb.GetLease() != nil
}

func (m *WorkerMessage) LeaseUpdate() (LeaseUpdateMessage, bool) {
	if !m.IsLeaseUpdate() {
		return LeaseUpdateMessage{}, false
	}
	return LeaseUpdateMessage{pb: m.pb.GetLease()}, true
}

func (m *WorkerMessage) IsDisconnect() bool {
	return m.pb.GetDisconnect() != nil
}

func (m *WorkerMessage) Disconnect() (DisconnectMessage, bool) {
	if !m.IsDisconnect() {
		return DisconnectMessage{}, false
	}
	return DisconnectMessage{pb: m.pb.GetDisconnect()}, true
}

func (m *WorkerMessage) IsAssign() bool {
	return m.pb.GetAssign() != nil
}

func (m *WorkerMessage) Assign() (AssignMessage, bool) {
	if !m.IsAssign() {
		return AssignMessage{}, false
	}
	return AssignMessage{pb: m.pb.GetAssign()}, true
}

func (m *WorkerMessage) IsRevoke() bool {
	return m.pb.GetRevoke() != nil
}

func (m *WorkerMessage) Revoke() (RevokeMessage, bool) {
	if !m.IsRevoke() {
		return RevokeMessage{}, false
	}
	return RevokeMessage{pb: m.pb.GetRevoke()}, true
}

func (m *WorkerMessage) IsRelinquished() bool {
	return m.pb.GetRelinquished() != nil
}

func (m *WorkerMessage) Relinquished() (RelinquishedMessage, bool) {
	if !m.IsRelinquished() {
		return RelinquishedMessage{}, false
	}
	return RelinquishedMessage{pb: m.pb.GetRelinquished()}, true
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
