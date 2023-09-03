package raft

import (
	"context"
	"go.atoms.co/lib/log"
	"go.atoms.co/slicex"
	"go.atoms.co/splitter/pkg/core"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pkg/storage/memory"
	"go.atoms.co/splitter/pb/private"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/raft"
	"io"
	"sync"
)

var (
	_ raft.FSM = (*FSM)(nil)
)

type FSM struct {
	storage *memory.Storage
	mu      sync.Mutex
}

func NewFSM(storage *memory.Storage) (*FSM, error) {
	return &FSM{
		storage: storage,
	}, nil
}

func (f *FSM) Apply(l *raft.Log) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()

	var pb internal_v1.RaftCommand
	if err := proto.Unmarshal(l.Data, &pb); err != nil {
		log.Fatalf(context.Background(), "Internal: invalid raft update: %v", err)
	}

	switch {
	case pb.GetTenant() != nil:
		resp, err := f.handleTenantCommand(pb.GetTenant())
		if err != nil {
			return &internal_v1.RaftResponse{
				Failed: true,
				Error:  err.Error(),
			}
		}
		return &internal_v1.RaftResponse{Resp: &internal_v1.RaftResponse_Tenant{Tenant: resp}}
	case pb.GetDomain() != nil:
		resp, err := f.handleDomainCommand(pb.GetDomain())
		if err != nil {
			return &internal_v1.RaftResponse{
				Failed: true,
				Error:  err.Error(),
			}
		}
		return &internal_v1.RaftResponse{Resp: &internal_v1.RaftResponse_Domain{Domain: resp}}
	case pb.GetPlacement() != nil:
		resp, err := f.handlePlacementCommand(pb.GetPlacement())
		if err != nil {
			return &internal_v1.RaftResponse{
				Failed: true,
				Error:  err.Error(),
			}
		}
		return &internal_v1.RaftResponse{Resp: &internal_v1.RaftResponse_Placement{Placement: resp}}
	default:
		panic(fmt.Sprintf("Internal: unknown raft command %v", pb))
	}
}

func (f *FSM) handleTenantCommand(command *internal_v1.TenantRaftCommand) (*internal_v1.TenantRaftResponse, error) {
	switch {
	case command.GetNew() != nil:
		err := f.storage.Tenants().New(context.Background(), model.WrapTenant(command.GetNew().GetTenant()))
		return &internal_v1.TenantRaftResponse{}, err
	case command.GetUpdate() != nil:
		info, err := f.storage.Tenants().Update(context.Background(), model.WrapTenant(command.GetUpdate().GetTenant()), model.Version(command.GetUpdate().GetVersion()))
		return core.NewTenantRaftResponseUpdateTenant(info), err
	case command.GetDelete() != nil:
		err := f.storage.Tenants().Delete(context.Background(), model.TenantName(command.GetDelete().GetName()))
		return &internal_v1.TenantRaftResponse{}, err
	default:
		panic(fmt.Sprintf("Internal: tenant domain raft command %v", command))
	}
}

func (f *FSM) handleDomainCommand(command *internal_v1.DomainRaftCommand) (*internal_v1.DomainRaftResponse, error) {
	var err error
	switch {
	case command.GetNew() != nil:
		err = f.storage.Domains().New(context.Background(), model.WrapDomain(command.GetNew().GetDomain()))
		return &internal_v1.DomainRaftResponse{}, err
	case command.GetUpdate() != nil:
		info, err := f.storage.Domains().Update(context.Background(), model.WrapDomain(command.GetUpdate().GetDomain()), model.Version(command.GetUpdate().GetVersion()))
		return core.NewDomainRaftResponseUpdateDomain(info), err
	case command.GetDelete() != nil:
		// TODO(jhhurwitz): 08/21/2023 Should we handle error here?
		name, _ := model.ParseQualifiedDomainName(command.GetDelete().GetName())
		err = f.storage.Domains().Delete(context.Background(), name)
		return &internal_v1.DomainRaftResponse{}, err
	default:
		panic(fmt.Sprintf("Internal: unknown domain raft command %v", command))
	}
}

func (f *FSM) handlePlacementCommand(command *internal_v1.PlacementRaftCommand) (*internal_v1.PlacementRaftResponse, error) {
	switch {
	case command.GetNew() != nil:
		info, err := f.storage.Placements().Create(context.Background(), core.WrapInternalPlacement(command.GetNew().GetPlacement()))
		return core.NewPlacementRaftResponseNewPlacement(info), err
	case command.GetUpdate() != nil:
		info, err := f.storage.Placements().Update(context.Background(), core.WrapInternalPlacement(command.GetUpdate().GetPlacement()), model.Version(command.GetUpdate().GetVersion()))
		return core.NewPlacementRaftResponseUpdatePlacement(info), err
	case command.GetDelete() != nil:
		// TODO(jhhurwitz): 08/21/2023 Should we handle error here?
		name, _ := model.ParseQualifiedPlacementName(command.GetDelete().GetName())
		err := f.storage.Placements().Delete(context.Background(), name)
		return &internal_v1.PlacementRaftResponse{}, err
	default:
		panic(fmt.Sprintf("Internal: unknown placement raft command %v", command))
	}
}

func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	tenants, err := f.storage.Tenants().List(context.Background())
	if err != nil {
		return &fsmSnapshot{}, err
	}

	var domains []model.DomainInfo
	for _, tenant := range tenants {
		d, err := f.storage.Domains().List(context.Background(), tenant.Name())
		if err != nil {
			return &fsmSnapshot{}, err
		}
		domains = append(domains, d...)
	}

	var placements []core.InternalPlacementInfo
	for _, tenant := range tenants {
		p, err := f.storage.Placements().List(context.Background(), tenant.Name())
		if err != nil {
			return &fsmSnapshot{}, err
		}
		placements = append(placements, p...)
	}

	return newFSMSnapshot(tenants, domains, placements), nil
}

func (f *FSM) Restore(snapshot io.ReadCloser) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	buf, err := io.ReadAll(snapshot)
	if err != nil {
		return err
	}

	var snap internal_v1.RaftSnapshot
	err = proto.Unmarshal(buf, &snap)
	if err != nil {
		return err
	}

	f.storage.Restore(
		slicex.Map(snap.GetTenants(), model.WrapTenantInfo),
		slicex.Map(snap.GetDomains(), model.WrapDomainInfo),
		slicex.Map(snap.GetPlacements(), core.WrapInternalPlacementInfo))

	return nil
}

type fsmSnapshot struct {
	tenants    []model.TenantInfo
	domains    []model.DomainInfo
	placements []core.InternalPlacementInfo
}

func newFSMSnapshot(tenants []model.TenantInfo, domains []model.DomainInfo, placements []core.InternalPlacementInfo) *fsmSnapshot {
	return &fsmSnapshot{
		tenants:    tenants,
		domains:    domains,
		placements: placements,
	}
}

func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		snapshot := &internal_v1.RaftSnapshot{
			Tenants:    slicex.Map(f.tenants, model.UnwrapTenantInfo),
			Domains:    slicex.Map(f.domains, model.UnwrapDomainInfo),
			Placements: slicex.Map(f.placements, core.UnwrapInternalPlacementInfo),
		}
		msg, err := proto.Marshal(snapshot)
		if err != nil {
			return err
		}
		if _, err := sink.Write(msg); err != nil {
			return err
		}
		// Close the sink.
		return sink.Close()
	}()

	if err != nil {
		sink.Cancel()
	}

	return err
}

func (f *fsmSnapshot) Release() {}
