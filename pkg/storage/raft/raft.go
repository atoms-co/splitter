package raft

import (
	"context"
	"atoms.co/lib-go/pkg/clock"
	"go.atoms.co/lib/log"
	"go.atoms.co/splitter/pkg/core"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pkg/storage"
	"go.atoms.co/splitter/pb/private"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/raft"
	"time"
)

var (
	ErrNotLeader = fmt.Errorf("not the raft leader")

	_ storage.Tenants    = (*tenants)(nil)
	_ storage.Domains    = (*domains)(nil)
	_ storage.Placements = (*placements)(nil)
)

const (
	DefaultCommandDeadline = 15 * time.Second
)

type Storage struct {
	cl clock.Clock

	// raftID is the ID of the raft instance in the cluster
	raftID raft.ServerID

	// fsm is the state store for Splitter data
	fsm *FSM

	// raft is the instance of raft we will operate on.
	raft *raft.Raft
}

func New(cl clock.Clock, raftID raft.ServerID, raft *raft.Raft, fsm *FSM) *Storage {
	s := &Storage{
		cl:     cl,
		raftID: raftID,
		raft:   raft,
		fsm:    fsm,
	}

	return s
}

func (s *Storage) Tenants() storage.Tenants {
	return (*tenants)(s)
}

func (s *Storage) Domains() storage.Domains {
	return (*domains)(s)
}

func (s *Storage) Placements() storage.Placements {
	return (*placements)(s)
}

type tenants Storage

func (t *tenants) List(ctx context.Context) ([]model.TenantInfo, error) {
	if !isLeader(t.raftID, t.raft) {
		return nil, ErrNotLeader
	}
	return t.fsm.storage.Tenants().List(ctx)
}

func (t *tenants) New(ctx context.Context, tenant model.Tenant) error {
	if !isLeader(t.raftID, t.raft) {
		return ErrNotLeader
	}
	_, err := applyRaft(ctx, core.NewRaftCommandNewTenant(tenant), t.cl, t.raft)
	return err
}

func (t *tenants) Read(ctx context.Context, name model.TenantName) (model.TenantInfo, error) {
	if !isLeader(t.raftID, t.raft) {
		return model.TenantInfo{}, ErrNotLeader
	}
	return t.fsm.storage.Tenants().Read(ctx, name)
}

func (t *tenants) Update(ctx context.Context, tenant model.Tenant, guard model.Version) (model.TenantInfo, error) {
	if !isLeader(t.raftID, t.raft) {
		return model.TenantInfo{}, ErrNotLeader
	}
	resp, err := applyRaft(ctx, core.NewRaftCommandUpdateTenant(tenant, guard), t.cl, t.raft)
	if err != nil {
		return model.TenantInfo{}, err
	}
	return model.WrapTenantInfo(resp.GetTenant().GetUpdate().GetTenant()), nil
}

func (t *tenants) Delete(ctx context.Context, name model.TenantName) error {
	if !isLeader(t.raftID, t.raft) {
		return ErrNotLeader
	}
	_, err := applyRaft(ctx, core.NewRaftCommandDeleteTenant(name), t.cl, t.raft)
	return err
}

type domains Storage

func (d *domains) List(ctx context.Context, tenant model.TenantName) ([]model.DomainInfo, error) {
	if !isLeader(d.raftID, d.raft) {
		return nil, ErrNotLeader
	}
	return d.fsm.storage.Domains().List(ctx, tenant)
}

func (d *domains) New(ctx context.Context, domain model.Domain) error {
	if !isLeader(d.raftID, d.raft) {
		return ErrNotLeader
	}
	_, err := applyRaft(ctx, core.NewRaftCommandNewDomain(domain), d.cl, d.raft)
	return err
}

func (d *domains) Read(ctx context.Context, name model.QualifiedDomainName) (model.DomainInfo, error) {
	if !isLeader(d.raftID, d.raft) {
		return model.DomainInfo{}, ErrNotLeader
	}
	return d.fsm.storage.Domains().Read(ctx, name)
}

func (d *domains) Update(ctx context.Context, domain model.Domain, guard model.Version) (model.DomainInfo, error) {
	if !isLeader(d.raftID, d.raft) {
		return model.DomainInfo{}, ErrNotLeader
	}
	resp, err := applyRaft(ctx, core.NewRaftCommandUpdateDomain(domain, guard), d.cl, d.raft)
	if err != nil {
		return model.DomainInfo{}, err
	}
	return model.WrapDomainInfo(resp.GetDomain().GetUpdate().GetDomain()), nil
}

func (d *domains) Delete(ctx context.Context, name model.QualifiedDomainName) error {
	if !isLeader(d.raftID, d.raft) {
		return ErrNotLeader
	}
	_, err := applyRaft(ctx, core.NewRaftCommandDeleteDomain(name), d.cl, d.raft)
	return err
}

type placements Storage

func (p *placements) List(ctx context.Context, tenant model.TenantName) ([]core.InternalPlacementInfo, error) {
	if !isLeader(p.raftID, p.raft) {
		return nil, ErrNotLeader
	}
	return p.fsm.storage.Placements().List(ctx, tenant)
}

func (p *placements) Create(ctx context.Context, placement core.InternalPlacement) (core.InternalPlacementInfo, error) {
	if !isLeader(p.raftID, p.raft) {
		return core.InternalPlacementInfo{}, ErrNotLeader
	}
	resp, err := applyRaft(ctx, core.NewRaftCommandNewPlacement(placement), p.cl, p.raft)
	if err != nil {
		return core.InternalPlacementInfo{}, err
	}
	return core.WrapInternalPlacementInfo(resp.GetPlacement().GetNew().GetPlacement()), nil
}

func (p *placements) Read(ctx context.Context, name model.QualifiedPlacementName) (core.InternalPlacementInfo, error) {
	if !isLeader(p.raftID, p.raft) {
		return core.InternalPlacementInfo{}, ErrNotLeader
	}
	return p.fsm.storage.Placements().Read(ctx, name)
}

func (p *placements) Update(ctx context.Context, placement core.InternalPlacement, guard model.Version) (core.InternalPlacementInfo, error) {
	if !isLeader(p.raftID, p.raft) {
		return core.InternalPlacementInfo{}, ErrNotLeader
	}
	resp, err := applyRaft(ctx, core.NewRaftCommandUpdatePlacement(placement, guard), p.cl, p.raft)
	if err != nil {
		return core.InternalPlacementInfo{}, err
	}
	return core.WrapInternalPlacementInfo(resp.GetPlacement().GetUpdate().GetPlacement()), nil
}

func (p *placements) Delete(ctx context.Context, name model.QualifiedPlacementName) error {
	if !isLeader(p.raftID, p.raft) {
		return ErrNotLeader
	}
	_, err := applyRaft(ctx, core.NewRaftCommandDeletePlacement(name), p.cl, p.raft)
	return err
}

func applyRaft(ctx context.Context, command *internal_v1.RaftCommand, cl clock.Clock, raft *raft.Raft) (*internal_v1.RaftResponse, error) {
	buf, err := proto.Marshal(command)
	if err != nil {
		return nil, err
	}

	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = cl.Now().Add(DefaultCommandDeadline)
	}

	resp := raft.Apply(buf, cl.Until(deadline))
	if resp.Error() != nil {
		return nil, resp.Error()
	}

	raftResp, ok := resp.Response().(*internal_v1.RaftResponse)
	if !ok {
		log.Fatalf(ctx, "Internal: invalid FSM response: %v", resp)
	}
	if raftResp.Failed {
		return nil, fmt.Errorf(raftResp.Error)
	}

	return raftResp, nil
}

func isLeader(raftID raft.ServerID, raft *raft.Raft) bool {
	_, leaderID := raft.LeaderWithID()
	return raftID == leaderID
}
