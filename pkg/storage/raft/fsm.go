package raft

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/hashicorp/raft"

	"go.atoms.co/lib/encoding/protox"
	"go.atoms.co/lib/log"
	"go.atoms.co/lib/metrics"
	splitterprivatepb "go.atoms.co/splitter/pb/private"
	"go.atoms.co/splitter/pkg/core"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pkg/storage"
)

var (
	_ raft.FSM = (*FSM)(nil)
)

var (
	// messageSizeBucketOpts contains the buckets for message size (in bytes) for fsm messages
	messageSizeBucketOpts = &metrics.BucketOptions{
		UserDefinedBuckets: []float64{
			1024,       // 1Kb
			4096,       // 4Kb
			16_384,     // 16Kb
			65_536,     // 64Kb
			262_144,    // 256Kb
			1_048_576,  // 1Mb
			4_194_304,  // 4Mb
			16_777_216, // 16Mb
			67_108_864, // 64Mb
		},
		DistributionType: metrics.UserDefined,
	}
)

var (
	actionLatency = metrics.NewHistogram("go.atoms.co/splitter/storage_raft_fsm_action_latency", "Raft FSM action latency", nil, core.ActionKey, core.ResultKey)
	messageSize   = metrics.NewByteHistogram("go.atoms.co/splitter/storage_raft_message_size", "Raft FSM message size", messageSizeBucketOpts, core.TenantKey, core.MessageTypeKey)
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
	now := time.Now()

	f.mu.Lock()
	defer f.mu.Unlock()

	// The FSM requires logical Apply errors to be handled upstream, so we should panic here if we encounter
	// invalid updates/deletes per https://github.com/hashicorp/raft/issues/307. However, Splitter can re-create
	// its data, so we skip bad updates instead.

	pb, err := protox.Unmarshal[splitterprivatepb.Mutation](l.Data)
	if err != nil {
		log.Errorf(context.Background(), "Internal: invalid raft mutation %v @%v: %v", l.Index, l.AppendedAt, err)

		recordActionLatency(time.Since(now), "apply/unmarshal", err)
		return nil
	}

	// TODO(herohde) 9/13/2023: Should we accept duplicate applies? I.e, if we retry an apply due
	// to timeout, say, can we see it twice? For now, we treat it as data corruption.

	switch {
	case pb.GetUpdate() != nil:
		upd := core.WrapUpdate(pb.GetUpdate())
		err := f.db.Update(upd, false)
		if err != nil {
			log.Errorf(context.Background(), "Internal: invalid raft update %v @%v: %v: %v", l.Index, l.AppendedAt, protox.MarshalTextString(pb), err)
		}

		recordActionLatency(time.Since(now), "apply/update", err)
		recordMessageSize(upd.Size(), upd.Name(), "apply/update")
		return nil

	case pb.GetDelete() != nil:
		del := core.WrapDelete(pb.GetDelete())
		err := f.db.Delete(del)
		if err != nil {
			log.Errorf(context.Background(), "Internal: invalid raft delete %v @%v: %v: %v", l.Index, l.AppendedAt, protox.MarshalTextString(pb), err)
		}

		recordActionLatency(time.Since(now), "apply/delete", err)
		recordMessageSize(del.Size(), del.Tenant(), "apply/delete")
		return nil

	case pb.GetRestore() != nil:
		restore := core.WrapRestore(pb.GetRestore())
		f.db.Restore(restore.Snapshot())

		recordActionLatency(time.Since(now), "apply/restore", err)
		recordMessageSize(restore.Size(), "_all", "apply/restore")
		return nil

	default:
		log.Errorf(context.Background(), "Internal: unknown raft mutation %v @%v: %v", l.Index, l.AppendedAt, protox.MarshalTextString(pb))

		recordActionLatency(time.Since(now), "apply/unknown", model.ErrInvalid)
		return nil
	}
}

func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	now := time.Now()

	f.mu.Lock()
	defer f.mu.Unlock()

	snap := f.db.Snapshot()

	recordActionLatency(time.Since(now), "snapshot", nil)
	recordMessageSize(snap.Size(), "_all", "snapshot")
	return &fsmSnapshot{data: snap}, nil
}

func (f *FSM) Restore(snapshot io.ReadCloser) error {
	now := time.Now()

	f.mu.Lock()
	defer f.mu.Unlock()

	buf, err := io.ReadAll(snapshot)
	if err != nil {
		log.Errorf(context.Background(), "Failed to read raft snapshot: %v", err)

		recordActionLatency(time.Since(now), "restore/read", err)
		return err
	}
	pb, err := protox.Unmarshal[splitterprivatepb.Snapshot](buf)
	if err != nil {
		log.Errorf(context.Background(), "Failed to unmarshal raft snapshot: %v", err)

		recordActionLatency(time.Since(now), "restore/unmarshal", err)
		return err
	}

	snap := core.WrapSnapshot(pb)
	f.db.Restore(snap)

	log.Infof(context.Background(), "Restored raft snapshot with size %v", len(buf))

	recordActionLatency(time.Since(now), "restore", nil)

	recordMessageSize(len(buf), "_all", "restore")
	for _, s := range snap.Tenants() {
		recordMessageSize(s.Size(), s.Tenant().Name(), "restore")
	}

	return nil
}

type fsmSnapshot struct {
	data core.Snapshot
}

func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	now := time.Now()
	err := f.persist(sink)
	recordActionLatency(time.Since(now), "persist", err)
	return err
}

func (f *fsmSnapshot) persist(sink raft.SnapshotSink) error {
	if err := f.tryPersist(sink); err != nil {
		log.Errorf(context.Background(), "Failed to persist raft snapshot on sink %v: %v", sink.ID(), err)
		_ = sink.Cancel()
		return err
	}

	log.Infof(context.Background(), "Persisted raft snapshot on sink %v", sink.ID())
	return nil
}

func (f *fsmSnapshot) tryPersist(sink raft.SnapshotSink) error {
	buf, err := protox.Marshal(core.UnwrapSnapshot(f.data))
	if err != nil {
		return err
	}
	if _, err := sink.Write(buf); err != nil {
		return err
	}
	return sink.Close()
}

func (f *fsmSnapshot) Release() {}

func recordActionLatency(latency time.Duration, action string, err error) {
	actionLatency.Observe(context.Background(), latency, core.ActionTag(action), core.ResultErrorTag(err))
}

func recordMessageSize(size int, tenant model.TenantName, msgType string) {
	messageSize.Observe(context.Background(), float64(size), core.TenantTag(tenant), core.MessageTypeTag(msgType))
}
