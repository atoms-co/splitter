package raft

import (
	"context"
	"io"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/raft"

	"go.atoms.co/lib/log"
	"go.atoms.co/lib/metrics"
	"go.atoms.co/lib/protox"
	"go.atoms.co/splitter/pkg/core"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pkg/storage"
	"go.atoms.co/splitter/pb/private"
)

var (
	_ raft.FSM = (*FSM)(nil)
)

var (
	numActions = metrics.NewCounter("go.atoms.co/splitter/storage_raft_fsm_actions", "Raft FSM actions", core.ActionKey, core.ResultKey)
)

// FSM implements the finite state machine logic needed for deterministic state propagation. It
// is a thin wrapper over the in-memory representation of management data. Thread-safe.
type FSM struct {
	db *storage.Cache
	mu sync.Mutex
}

func NewFSM() *FSM {
	return &FSM{db: storage.NewCache()}
}

func (f *FSM) Read() core.Snapshot {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.db.Snapshot()
}

func (f *FSM) Apply(l *raft.Log) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()

	// The FSM requires logical Apply errors to be handled upstream, so we should panic here if we encounter
	// invalid updates/deletes per https://github.com/hashicorp/raft/issues/307. However, Splitter can re-create
	// its data, so we skip bad updates instead.

	pb, err := protox.Unmarshal[internal_v1.Mutation](l.Data)
	if err != nil {
		log.Errorf(context.Background(), "Internal: invalid raft mutation %v @%v: %v", l.Index, l.AppendedAt, err)

		recordAction("apply/unmarshal", err)
		return nil
	}

	// TODO(herohde) 9/13/2023: Should we accept duplicate applies? I.e, if we retry an apply due
	// to timeout, say, can we see it twice? For now, we treat it as data corruption.

	switch {
	case pb.GetUpdate() != nil:
		err := f.db.Update(core.WrapUpdate(pb.GetUpdate()), false)
		if err != nil {
			log.Errorf(context.Background(), "Internal: invalid raft update %v @%v: %v: %v", l.Index, l.AppendedAt, proto.MarshalTextString(pb), err)
		}

		recordAction("apply/update", err)
		return nil

	case pb.GetDelete() != nil:
		err := f.db.Delete(core.WrapDelete(pb.GetDelete()))
		if err != nil {
			log.Errorf(context.Background(), "Internal: invalid raft delete %v @%v: %v: %v", l.Index, l.AppendedAt, proto.MarshalTextString(pb), err)
		}

		recordAction("apply/delete", err)
		return nil

	case pb.GetRestore() != nil:
		f.db.Restore(core.WrapRestore(pb.GetRestore()).Snapshot())

		recordAction("apply/restore", err)
		return nil

	default:
		log.Errorf(context.Background(), "Internal: unknown raft mutation %v @%v: %v", l.Index, l.AppendedAt, proto.MarshalTextString(pb))

		recordAction("apply/unknown", model.ErrInvalid)
		return nil
	}
}

func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	recordAction("snapshot", nil)
	return &fsmSnapshot{data: f.db.Snapshot()}, nil
}

func (f *FSM) Restore(snapshot io.ReadCloser) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	buf, err := io.ReadAll(snapshot)
	if err != nil {
		log.Errorf(context.Background(), "Failed to read raft snapshot: %v", err)

		recordAction("restore/read", err)
		return err
	}
	pb, err := protox.Unmarshal[internal_v1.Snapshot](buf)
	if err != nil {
		log.Errorf(context.Background(), "Failed to unmarshal raft snapshot: %v", err)

		recordAction("restore/unmarshal", err)
		return err
	}

	f.db.Restore(core.WrapSnapshot(pb))

	log.Infof(context.Background(), "Restored raft snapshot")

	recordAction("restore", nil)
	return nil
}

type fsmSnapshot struct {
	data core.Snapshot
}

func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := f.persist(sink)
	recordAction("persist", err)
	return err
}

func (f *fsmSnapshot) persist(sink raft.SnapshotSink) error {
	if err := f.tryPersist(sink); err != nil {
		log.Errorf(context.Background(), "Failed to persist raft snapshot on sink %v: %v", sink.ID(), err)
		_ = sink.Cancel()
		return err
	}

	log.Infof(context.Background(), "Persisted raft snapshot on sink %v: %v", sink.ID())
	return nil
}

func (f *fsmSnapshot) tryPersist(sink raft.SnapshotSink) error {
	buf, err := proto.Marshal(core.UnwrapSnapshot(f.data))
	if err != nil {
		return err
	}
	if _, err := sink.Write(buf); err != nil {
		return err
	}
	return sink.Close()
}

func (f *fsmSnapshot) Release() {}

func recordAction(action string, err error) {
	numActions.Increment(context.Background(), 1, core.ActionTag(action), core.ResultErrorTag(err))
}
