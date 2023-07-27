package raftstorage

import (
	"context"
	"atoms.co/lib-go/pkg/clock"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pkg/storage"
	"github.com/hashicorp/raft"
)

var (
	_ storage.Tenants = (*tenants)(nil)
)

type Storage struct {
	cl clock.Clock

	// fsm is the state store for Splitter data
	fsm *FSM

	// raft is the instance of raft we will operate on.
	raft *raft.Raft
}

func New(cl clock.Clock, fsm *FSM, raftObj *raft.Raft) (*Storage, storage.Management) {
	d := &Storage{
		cl:   cl,
		fsm:  fsm,
		raft: raftObj,
	}

	return d, storage.Management{
		Tenants: (*tenants)(d),
	}
}

type tenants Storage

func (t *tenants) List(ctx context.Context) ([]model.TenantInfo, error) {
	//TODO implement me
	panic("implement me")
}

func (t *tenants) New(ctx context.Context, tenant model.Tenant) error {
	//TODO implement me
	panic("implement me")
}

func (t *tenants) Read(ctx context.Context, name model.TenantName) (model.TenantInfo, error) {
	//TODO implement me
	panic("implement me")
}

func (t *tenants) Update(ctx context.Context, tenant model.Tenant, guard storage.UpdateGuard) error {
	//TODO implement me
	panic("implement me")
}

func (t *tenants) Delete(ctx context.Context, name model.TenantName) error {
	//TODO implement me
	panic("implement me")
}
