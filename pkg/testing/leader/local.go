package leader

import (
	"context"
	"atoms.co/lib-go/pkg/clock"
	"go.atoms.co/splitter/lib/service/session"
	"go.atoms.co/lib/iox"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pkg/service/leader"
	"go.atoms.co/splitter/pb/private"
	"time"
)

// LocalManager is a leader manager that always uses local leader. Useful for testing.
type LocalManager struct {
	iox.AsyncCloser
	cl    clock.Clock
	local leader.Proxy
}

func NewLocalManager(cl clock.Clock, local leader.Proxy) *LocalManager {
	ret := &LocalManager{
		AsyncCloser: iox.NewAsyncCloser(),
		cl:          cl,
		local:       local,
	}
	return ret
}

func (m *LocalManager) Drain(timeout time.Duration) {
	m.cl.AfterFunc(timeout, m.Close)
}

func (m *LocalManager) Resolve(ctx context.Context, key model.DomainKey) (internal_v1.LeaderServiceClient, error) {
	return nil, model.ErrNoResolution
}

func (m *LocalManager) Join(ctx context.Context, sid session.ID, in <-chan leader.Message) (<-chan leader.Message, error) {
	return m.local.Join(ctx, sid, in)
}

func (m *LocalManager) Handle(ctx context.Context, request leader.HandleRequest) (*internal_v1.LeaderHandleResponse, error) {
	return m.local.Handle(ctx, request)
}
