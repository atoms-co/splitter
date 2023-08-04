package leader

import (
	"context"
	"go.atoms.co/splitter/lib/service/session"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pb/private"
)

// Proxy is a local proxy for accessing the leader, if present. Returns ErrNotOwned if not.
type Proxy interface {
	Join(ctx context.Context, sid session.ID, id model.Instance, grants []Grant, in <-chan JoinMessage) (<-chan JoinMessage, error)
	Handle(ctx context.Context, request HandleRequest) (*internal_v1.LeaderHandleResponse, error)
}

type HandleRequest struct {
	pb *internal_v1.LeaderHandleRequest
}
