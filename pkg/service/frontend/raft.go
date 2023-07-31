package frontend

import (
	"context"
	"atoms.co/lib-go/pkg/clock"
	"go.atoms.co/splitter/pb/private"
	"github.com/hashicorp/raft"
	"time"
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

func (r RaftService) Join(ctx context.Context, request *internal_v1.RaftJoinRequest) (*internal_v1.RaftJoinResponse, error) {
	// Must be run on leader
	f := r.raft.AddVoter(raft.ServerID(request.GetId()), raft.ServerAddress(request.GetAddr()), 0, 10*time.Second)
	return &internal_v1.RaftJoinResponse{}, f.Error()
}
