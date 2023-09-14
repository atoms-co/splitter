package raft

import (
	"context"
	"go.atoms.co/lib/log"
	"go.atoms.co/lib/protox"
	"go.atoms.co/splitter/pkg/storage"
	"go.atoms.co/splitter/pb/private"
	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/raft"
	"io"
	"sync"
)

var (
	_ raft.FSM = (*FSM)(nil)
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

func (f *FSM) Read() storage.Snapshot {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.db.Snapshot()
}

func (f *FSM) Apply(l *raft.Log) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()

	// The FSM requires logical Apply errors to be handled upstream, so we panic here if we encounter
	// invalid updates/deletes per https://github.com/hashicorp/raft/issues/307.

	pb, err := protox.Unmarshal[internal_v1.Mutation](l.Data)
	if err != nil {
		log.Fatalf(context.Background(), "Internal: invalid raft mutation: %v", err)
	}

	// TODO(hurwitz) 9/13/2023: We may want to consider a soft failure here, to avoid a validation
	// bug in the leader bricking Splitter.

	// TODO(herohde) 9/13/2023: Should we accept duplicate applies? I.e, if we retry an apply due
	// to timeout, say, can we see it twice?

	switch {
	case pb.GetUpdate() != nil:
		if err := f.db.Update(storage.WrapUpdate(pb.GetUpdate()), false); err != nil {
			log.Fatalf(context.Background(), "Internal: invalid raft update %v: %v", proto.MarshalTextString(pb), err)
		}
		return nil

	case pb.GetDelete() != nil:
		if err := f.db.Delete(storage.WrapDelete(pb.GetDelete())); err != nil {
			log.Fatalf(context.Background(), "Internal: invalid raft delete %v: %v", proto.MarshalTextString(pb), err)
		}
		return nil

	default:
		log.Fatalf(context.Background(), "Internal: unknown raft mutation: %v", proto.MarshalTextString(pb))
		return nil
	}
}

func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	return &fsmSnapshot{data: f.db.Snapshot()}, nil
}

func (f *FSM) Restore(snapshot io.ReadCloser) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	buf, err := io.ReadAll(snapshot)
	if err != nil {
		log.Errorf(context.Background(), "Failed to read raft snapshot: %v", err)
		return err
	}
	pb, err := protox.Unmarshal[internal_v1.Snapshot](buf)
	if err != nil {
		log.Errorf(context.Background(), "Failed to unmarshal raft snapshot: %v", err)
		return err
	}

	f.db.Restore(storage.WrapSnapshot(pb))

	log.Debugf(context.Background(), "Restored raft snapshot")
	return nil
}

type fsmSnapshot struct {
	data storage.Snapshot
}

func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	if err := f.tryPersist(sink); err != nil {
		log.Errorf(context.Background(), "Failed to persist raft snapshot on sink %v: %v", sink.ID(), err)
		_ = sink.Cancel()
		return err
	}

	log.Debugf(context.Background(), "Persisted raft snapshot on sink %v: %v", sink.ID())
	return nil
}

func (f *fsmSnapshot) tryPersist(sink raft.SnapshotSink) error {
	buf, err := proto.Marshal(storage.UnwrapSnapshot(f.data))
	if err != nil {
		return err
	}
	if _, err := sink.Write(buf); err != nil {
		return err
	}
	return sink.Close()
}

func (f *fsmSnapshot) Release() {}
