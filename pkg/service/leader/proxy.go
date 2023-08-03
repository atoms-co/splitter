package leader

import (
	"context"
	"go.atoms.co/splitter/pb/private"
)

// Proxy is a leader proxy.
type Proxy interface {
	Join(ctx context.Context, in <-chan JoinMessage) (<-chan JoinMessage, error)
	Handle(ctx context.Context, request HandleRequest) (internal_v1.LeaderHandleResponse, error)
}
