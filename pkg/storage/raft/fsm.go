package raft

import (
	"context"
	"go.atoms.co/lib/log"
	"go.atoms.co/slicex"
	"go.atoms.co/splitter/pkg/core"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pkg/storage/memory"
	"go.atoms.co/splitter/pb/private"
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

	var err error
	switch {
	case pb.GetTenant() != nil:
		err = f.handleTenantCommand(pb.GetTenant())
	case pb.GetPlacement() != nil:
		err = f.handlePlacementCommand(pb.GetPlacement())
	default:
		log.Fatalf(context.Background(), "Internal: unknown raft update %v", pb)
	}
	if err != nil {
		return &internal_v1.RaftResponse{
			Succeeded: false,
			Error:     err.Error(),
		}
	}

	return &internal_v1.RaftResponse{
		Succeeded: true,
	}
}

func (f *FSM) handlePlacementCommand(placement *internal_v1.PlacementCommand) error {
	switch {
	case placement.GetNew() != nil:
	case placement.GetUpdate() != nil:
	case placement.GetDelete() != nil:
	default:
		log.Fatalf(context.Background(), "Internal: unknown placement raft update %v", placement)
	}
	return nil
}

func (f *FSM) handleTenantCommand(upd *internal_v1.TenantCommand) error {
	var err error
	switch {
	case upd.GetNew() != nil:
		err = f.storage.Tenants().New(context.Background(), model.WrapTenant(upd.GetNew().GetTenant()))
	case upd.GetUpdate() != nil:
		err = f.storage.Tenants().Update(context.Background(), model.WrapTenant(upd.GetUpdate().GetTenant()), model.Version(upd.GetUpdate().GetVersion()))
	case upd.GetDelete() != nil:
		err = f.storage.Tenants().Delete(context.Background(), model.TenantName(upd.GetDelete().GetName()))
	default:
		log.Fatalf(context.Background(), "Internal: unknown tenant raft update %v", upd)
	}
	return err
}

func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	tenants, err := f.storage.Tenants().List(context.Background())
	if err != nil {
		return &fsmSnapshot{}, err
	}

	var placements []core.InternalPlacementInfo
	for _, tenant := range tenants {
		p, err := f.storage.Placements().List(context.Background(), tenant.Name())
		if err != nil {
			return &fsmSnapshot{}, err
		}
		placements = append(placements, p...)
	}

	return &fsmSnapshot{
		tenants:    tenants,
		placements: placements,
	}, nil
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

	f.storage.Restore(slicex.Map(snap.GetTenants(), model.WrapTenantInfo), slicex.Map(snap.GetPlacements(), core.WrapInternalPlacementInfo))
	return nil
}

type fsmSnapshot struct {
	tenants    []model.TenantInfo
	placements []core.InternalPlacementInfo
}

func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		snapshot := &internal_v1.RaftSnapshot{
			Tenants:    slicex.Map(f.tenants, model.UnwrapTenantInfo),
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
