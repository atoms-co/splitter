package core

import (
	"go.atoms.co/slicex"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pb/private"
	"time"
)

type WorkerMessage struct {
	pb *internal_v1.WorkerMessage
}

func (m *WorkerMessage) IsRegister() bool {
	return m.pb.GetRegister() != nil
}

func (m *WorkerMessage) Register() (Register, bool) {
	if !m.IsRegister() {
		return Register{}, false
	}
	return Register{pb: m.pb.GetRegister()}, true
}

func (m *WorkerMessage) IsDeregister() bool {
	return m.pb.GetDeregister() != nil
}

func (m *WorkerMessage) Deregister() (Deregister, bool) {
	if !m.IsDeregister() {
		return Deregister{}, false
	}
	return Deregister{pb: m.pb.GetDeregister()}, true
}

func (m *WorkerMessage) IsLeaseUpdate() bool {
	return m.pb.GetLease() != nil
}

func (m *WorkerMessage) LeaseUpdate() (LeaseUpdate, bool) {
	if !m.IsLeaseUpdate() {
		return LeaseUpdate{}, false
	}
	return LeaseUpdate{pb: m.pb.GetLease()}, true
}

func (m *WorkerMessage) IsDisconnect() bool {
	return m.pb.GetDisconnect() != nil
}

func (m *WorkerMessage) Disconnect() (Disconnect, bool) {
	if !m.IsDisconnect() {
		return Disconnect{}, false
	}
	return Disconnect{pb: m.pb.GetDisconnect()}, true
}

func (m *WorkerMessage) IsAssign() bool {
	return m.pb.GetAssign() != nil
}

func (m *WorkerMessage) Assign() (Assign, bool) {
	if !m.IsAssign() {
		return Assign{}, false
	}
	return Assign{pb: m.pb.GetAssign()}, true
}

func (m *WorkerMessage) IsRevoke() bool {
	return m.pb.GetRevoke() != nil
}

func (m *WorkerMessage) Revoke() (Revoke, bool) {
	if !m.IsRevoke() {
		return Revoke{}, false
	}
	return Revoke{pb: m.pb.GetRevoke()}, true
}

func (m *WorkerMessage) IsRelinquished() bool {
	return m.pb.GetRelinquished() != nil
}

func (m *WorkerMessage) Relinquished() (Relinquished, bool) {
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

func (m Register) Active() []Grant {
	return slicex.Map(m.pb.GetActive(), WrapGrant)
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

func (m Assign) Grant() Grant {
	return WrapGrant(m.pb.GetGrant())
}

type Revoke struct {
	pb *internal_v1.WorkerMessage_Revoke
}

func (m Revoke) Grants() []Grant {
	return slicex.Map(m.pb.GetGrants(), WrapGrant)
}

type Relinquished struct {
	pb *internal_v1.WorkerMessage_Relinquished
}

func (m Relinquished) Grants() []Grant {
	return slicex.Map(m.pb.GetGrants(), WrapGrant)
}
