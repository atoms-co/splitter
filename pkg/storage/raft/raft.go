package raft

import (
	"context"
	"atoms.co/lib-go/pkg/clock"
	"go.atoms.co/lib/log"
	"go.atoms.co/splitter/pkg/storage"
	"go.atoms.co/splitter/pb/private"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/raft"
	"time"
)

var (
	ErrNotLeader = fmt.Errorf("not the raft leader")

	_ storage.Storage = (*Storage)(nil)
)

const (
	// applyDeadline is the deadline for apply I/O. If an apply takes longer, we time out
	// and are unsure whether it was applied or not.
	applyDeadline = 15 * time.Second
)

// Storage provides raft-based Storage. It can be used on the raft leader only.
type Storage struct {
	cl clock.Clock

	raftID raft.ServerID
	raft   *raft.Raft
	fsm    *FSM
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

func (s *Storage) Read(ctx context.Context) (storage.Snapshot, error) {
	if !s.isLeader() {
		return storage.Snapshot{}, ErrNotLeader
	}

	// Read is called only on the leader. We want to ensure that there are no
	// outstanding updates before the Splitter leader takes control over the state
	// in-memory, causing (potential) data corruption. Barrier provides that guarantee.

	if err := s.raft.Barrier(applyDeadline).Error(); err != nil {
		return storage.Snapshot{}, fmt.Errorf("failed to create barrier: %v", err)
	}
	return s.fsm.Read(), nil
}

func (s *Storage) Update(ctx context.Context, update storage.Update) error {
	if !s.isLeader() {
		return ErrNotLeader
	}

	return s.apply(ctx, &internal_v1.Mutation{
		Msg: &internal_v1.Mutation_Update{
			Update: storage.UnwrapUpdate(update),
		},
	})
}

func (s *Storage) Delete(ctx context.Context, del storage.Delete) error {
	if !s.isLeader() {
		return ErrNotLeader
	}

	return s.apply(ctx, &internal_v1.Mutation{
		Msg: &internal_v1.Mutation_Delete{
			Delete: storage.UnwrapDelete(del),
		},
	})
}

func (s *Storage) apply(ctx context.Context, mutation *internal_v1.Mutation) error {
	buf, err := proto.Marshal(mutation)
	if err != nil {
		log.Errorf(context.Background(), "Failed to marshal raft mutation: %v", proto.MarshalTextString(mutation), err)
		return err
	}

	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = s.cl.Now().Add(applyDeadline)
	}

	resp := s.raft.Apply(buf, s.cl.Until(deadline))
	if err := resp.Error(); err != nil {
		log.Errorf(context.Background(), "Failed to apply raft mutation %v: %v", proto.MarshalTextString(mutation), err)
		return err
	}
	return nil
}

func (s *Storage) isLeader() bool {
	_, leaderID := s.raft.LeaderWithID()
	return s.raftID == leaderID
}
