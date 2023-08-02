package raft

import (
	"context"
	"atoms.co/lib-go/pkg/clock"
	"go.atoms.co/splitter/pkg/core"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pkg/storage"
	"github.com/hashicorp/raft"
)

var (
	_ storage.Tenants    = (*tenants)(nil)
	_ storage.Domains    = (*domains)(nil)
	_ storage.Placements = (*placements)(nil)
)

type Storage struct {
	cl clock.Clock

	// fsm is the state store for Splitter data
	fsm *FSM

	// raft is the instance of raft we will operate on.
	raft *raft.Raft
}

func New(cl clock.Clock, fsm *FSM, raftObj *raft.Raft) (*Storage, storage.Storage) {
	d := &Storage{
		cl:   cl,
		fsm:  fsm,
		raft: raftObj,
	}

	return d, storage.Storage{
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

func (t *tenants) Update(ctx context.Context, tenant model.Tenant, guard model.Version) error {
	//TODO implement me
	panic("implement me")
}

func (t *tenants) Delete(ctx context.Context, name model.TenantName) error {
	//TODO implement me
	panic("implement me")
}

type domains Storage

func (d *domains) List(ctx context.Context) ([]model.Domain, error) {
	//TODO implement me
	panic("implement me")
}

func (d *domains) New(ctx context.Context, domain model.Domain) error {
	//TODO implement me
	panic("implement me")
}

func (d *domains) Read(ctx context.Context, name model.QualifiedDomainName) (model.Domain, error) {
	//TODO implement me
	panic("implement me")
}

func (d *domains) Update(ctx context.Context, domain model.Domain, guard model.Version) error {
	//TODO implement me
	panic("implement me")
}

func (d *domains) Delete(ctx context.Context, name model.QualifiedDomainName) error {
	//TODO implement me
	panic("implement me")
}

type placements Storage

func (p *placements) List(ctx context.Context, tenant model.TenantName) ([]core.InternalPlacementInfo, error) {
	//TODO implement me
	panic("implement me")
}

func (p *placements) Create(ctx context.Context, placement core.InternalPlacement) (core.InternalPlacementInfo, error) {
	//TODO implement me
	panic("implement me")
}

func (p *placements) Read(ctx context.Context, name model.QualifiedPlacementName) (core.InternalPlacementInfo, error) {
	//TODO implement me
	panic("implement me")
}

func (p *placements) Update(ctx context.Context, placement core.InternalPlacement, guard model.Version) (core.InternalPlacementInfo, error) {
	//TODO implement me
	panic("implement me")
}

func (p *placements) Delete(ctx context.Context, name model.QualifiedPlacementName) error {
	//TODO implement me
	panic("implement me")
}
