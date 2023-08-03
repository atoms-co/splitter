package frontend

import (
	"context"
	"atoms.co/lib-go/pkg/clock"
	"go.atoms.co/splitter/pb/private"
)

type LeaderService struct {
	cl clock.Clock
}

func NewLeaderService(cl clock.Clock) *LeaderService {
	return &LeaderService{
		cl: cl,
	}
}

func (l *LeaderService) Join(server internal_v1.LeaderService_JoinServer) error {
	panic("implement me")
}

func (l *LeaderService) Handle(ctx context.Context, request *internal_v1.LeaderHandleRequest) (*internal_v1.LeaderHandleResponse, error) {
	return nil, nil
}
