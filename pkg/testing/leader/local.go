package leader

import (
	"context"
	"time"

	"go.atoms.co/splitter/lib/service/session"
	"go.atoms.co/iox"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pkg/service/leader"
	splitterprivatepb "go.atoms.co/splitter/pb/private"
)

// LocalManager is a leader manager that always uses local leader. Useful for testing.
type LocalManager struct {
	iox.AsyncCloser
	local leader.Proxy
}

func NewLocalManager(local leader.Proxy) *LocalManager {
	ret := &LocalManager{
		AsyncCloser: iox.NewAsyncCloser(),
		local:       local,
	}
	return ret
}

func (m *LocalManager) Drain(timeout time.Duration) {
	time.AfterFunc(timeout, m.Close)
}

func (m *LocalManager) Resolve(ctx context.Context, key model.DomainKey) (splitterprivatepb.LeaderServiceClient, error) {
	return nil, model.ErrNoResolution
}

func (m *LocalManager) Join(ctx context.Context, sid session.ID, in <-chan leader.Message) (<-chan leader.Message, error) {
	return m.local.Join(ctx, sid, in)
}

func (m *LocalManager) Handle(ctx context.Context, request leader.HandleRequest) (*splitterprivatepb.LeaderHandleResponse, error) {
	return m.local.Handle(ctx, request)
}
