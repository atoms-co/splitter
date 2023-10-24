package worker

import (
	"go.atoms.co/slicex"
	"go.atoms.co/splitter/pkg/core"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pb/private"
	"google.golang.org/protobuf/types/known/timestamppb"
	"time"
)

type Message struct {
	pb *internal_v1.WorkerMessage
}

func NewDisconnect() Message {
	return Message{pb: &internal_v1.WorkerMessage{
		Msg: &internal_v1.WorkerMessage_Disconnect_{
			Disconnect: &internal_v1.WorkerMessage_Disconnect{},
		},
	}}
}
func NewLeaseUpdateMessage(ttl time.Time) Message {
	return Message{pb: &internal_v1.WorkerMessage{
		Msg: &internal_v1.WorkerMessage_Lease{
			Lease: &internal_v1.WorkerMessage_LeaseUpdate{
				Ttl: timestamppb.New(ttl),
			},
		},
	}}
}

func NewAssign(grant core.Grant, state core.State) Message {
	return Message{pb: &internal_v1.WorkerMessage{
		Msg: &internal_v1.WorkerMessage_Assign_{
			Assign: &internal_v1.WorkerMessage_Assign{
				Grant: core.UnwrapGrant(grant),
				State: core.UnwrapState(state),
			},
		},
	}}
}

func NewRevoke(grants ...core.Grant) Message {
	return Message{pb: &internal_v1.WorkerMessage{
		Msg: &internal_v1.WorkerMessage_Revoke_{
			Revoke: &internal_v1.WorkerMessage_Revoke{
				Grants: slicex.Map(grants, core.UnwrapGrant),
			},
		},
	}}
}

func WrapMessage(pb *internal_v1.WorkerMessage) Message {
	return Message{pb: pb}
}

func UnwrapMessage(m Message) *internal_v1.WorkerMessage {
	return m.pb
}

func (m *Message) IsRegister() bool {
	return m.pb.GetRegister() != nil
}

func (m *Message) Register() (Register, bool) {
	if !m.IsRegister() {
		return Register{}, false
	}
	return Register{pb: m.pb.GetRegister()}, true
}

func (m *Message) IsDeregister() bool {
	return m.pb.GetDeregister() != nil
}

func (m *Message) Deregister() (Deregister, bool) {
	if !m.IsDeregister() {
		return Deregister{}, false
	}
	return Deregister{pb: m.pb.GetDeregister()}, true
}

func (m *Message) IsLeaseUpdate() bool {
	return m.pb.GetLease() != nil
}

func (m *Message) LeaseUpdate() (LeaseUpdate, bool) {
	if !m.IsLeaseUpdate() {
		return LeaseUpdate{}, false
	}
	return LeaseUpdate{pb: m.pb.GetLease()}, true
}

func (m *Message) IsDisconnect() bool {
	return m.pb.GetDisconnect() != nil
}

func (m *Message) Disconnect() (Disconnect, bool) {
	if !m.IsDisconnect() {
		return Disconnect{}, false
	}
	return Disconnect{pb: m.pb.GetDisconnect()}, true
}

func (m *Message) IsAssign() bool {
	return m.pb.GetAssign() != nil
}

func (m *Message) Assign() (Assign, bool) {
	if !m.IsAssign() {
		return Assign{}, false
	}
	return Assign{pb: m.pb.GetAssign()}, true
}

func (m *Message) IsRevoke() bool {
	return m.pb.GetRevoke() != nil
}

func (m *Message) Revoke() (Revoke, bool) {
	if !m.IsRevoke() {
		return Revoke{}, false
	}
	return Revoke{pb: m.pb.GetRevoke()}, true
}

func (m *Message) IsRelinquished() bool {
	return m.pb.GetRelinquished() != nil
}

func (m *Message) Relinquished() (Relinquished, bool) {
	if !m.IsRelinquished() {
		return Relinquished{}, false
	}
	return Relinquished{pb: m.pb.GetRelinquished()}, true
}

type Register struct {
	pb *internal_v1.WorkerMessage_Register
}

func (m Register) Worker() model.Instance {
	return model.WrapInstance(m.pb.GetWorker())
}

func (m Register) Active() []core.Grant {
	return slicex.Map(m.pb.GetActive(), core.WrapGrant)
}

type Deregister struct {
	pb *internal_v1.WorkerMessage_Deregister
}

type LeaseUpdate struct {
	pb *internal_v1.WorkerMessage_LeaseUpdate
}

func (m LeaseUpdate) Ttl() time.Time {
	return m.pb.GetTtl().AsTime()
}

type Disconnect struct {
	pb *internal_v1.WorkerMessage_Disconnect
}

type Assign struct {
	pb *internal_v1.WorkerMessage_Assign
}

func (m Assign) Grant() core.Grant {
	return core.WrapGrant(m.pb.GetGrant())
}

type Revoke struct {
	pb *internal_v1.WorkerMessage_Revoke
}

func (m Revoke) Grants() []core.Grant {
	return slicex.Map(m.pb.GetGrants(), core.WrapGrant)
}

type Relinquished struct {
	pb *internal_v1.WorkerMessage_Relinquished
}

func (m Relinquished) Grants() []core.Grant {
	return slicex.Map(m.pb.GetGrants(), core.WrapGrant)
}
