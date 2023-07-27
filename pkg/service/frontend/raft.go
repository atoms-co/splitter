package frontend

import (
	"context"
	"atoms.co/lib-go/pkg/clock"
	"go.atoms.co/splitter/pb/private"
	"github.com/hashicorp/raft"
)

type RaftService struct {
	cl   clock.Clock
	raft *raft.Raft
}

func NewRaftService(cl clock.Clock, raftObj *raft.Raft) *RaftService {
	return &RaftService{
		cl:   cl,
		raft: raftObj,
	}
}

func (r RaftService) Join(ctx context.Context, request *internal_v1.JoinRequest) (*internal_v1.JoinResponse, error) {
	//TODO implement me
	panic("implement me")
}
