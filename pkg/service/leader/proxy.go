package leader

import (
	"context"
	"go.atoms.co/splitter/lib/service/session"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pb/private"
	"github.com/golang/protobuf/proto"
)

// Proxy is a local proxy for accessing the leader, if present. Returns ErrNotOwned if not.
type Proxy interface {
	Join(ctx context.Context, sid session.ID, id model.Instance, grants []Grant, in <-chan JoinMessage) (<-chan JoinMessage, error)
	Handle(ctx context.Context, request HandleRequest) (*internal_v1.LeaderHandleResponse, error)
}

// HandleRequest is an internal leader handle request jacket for routing. Not threadsafe.
type HandleRequest struct {
	Proto *internal_v1.LeaderHandleRequest
}

func (m HandleRequest) String() string {
	return proto.CompactTextString(m.Proto)
}
