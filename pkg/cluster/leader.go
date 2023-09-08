package cluster

import (
	"context"
	"atoms.co/lib-go/pkg/clock"
	"go.atoms.co/splitter/lib/service/session"
	"go.atoms.co/lib/log"
	"go.atoms.co/lib/contextx"
	"go.atoms.co/lib/net/grpcx"
	"go.atoms.co/lib/iox"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pkg/service/leader"
	"go.atoms.co/splitter/pb/private"
	"github.com/hashicorp/raft"
	"sync"
	"time"
)

type LeaderFactory func(ctx context.Context) (iox.AsyncCloser, leader.Proxy)

type LeaderManager struct {
	iox.AsyncCloser

	cl clock.Clock
	id raft.ServerID

	factory LeaderFactory

	remote internal_v1.LeaderServiceClient // nil if local
	local  leader.Proxy                    // nil if remote
	halt   iox.AsyncCloser                 // stop signal for local/remote re-creation if active
	mu     sync.Mutex
}

func NewLeaderManager(cl clock.Clock, id raft.ServerID, in <-chan raft.LeaderObservation, factory LeaderFactory) *LeaderManager {
	ret := &LeaderManager{
		AsyncCloser: iox.NewAsyncCloser(),
		cl:          cl,
		id:          id,
		factory:     factory,
	}
	go ret.process(context.Background(), in)

	return ret
}

func (m *LeaderManager) Resolve(ctx context.Context, key model.DomainKey) (internal_v1.LeaderServiceClient, error) {
	if client, ok := m.tryRemote(); ok {
		return client, nil
	}
	return nil, model.ErrNotFound
}

func (m *LeaderManager) Join(ctx context.Context, sid session.ID, id model.Instance, grants []leader.Grant, in <-chan leader.JoinMessage) (<-chan leader.JoinMessage, error) {
	if l, ok := m.tryLocal(); ok {
		return l.Join(ctx, sid, id, grants, in)
	}
	return nil, model.ErrNotOwned
}

func (m *LeaderManager) Handle(ctx context.Context, request leader.HandleRequest) (*internal_v1.LeaderHandleResponse, error) {
	if l, ok := m.tryLocal(); ok {
		return l.Handle(ctx, request)
	}
	return nil, model.ErrNotOwned
}

func (m *LeaderManager) process(ctx context.Context, in <-chan raft.LeaderObservation) {
	defer m.Close()
	defer m.reset()

	for {
		select {
		case msg, ok := <-in:
			if !ok {
				return
			}
			halt := m.reset()

			// On leader change, async create or establish connection to leader. As long as
			// the leader has not changed again, re-creation may be necessary. The 'halt'
			// signal delimits that.

			if msg.LeaderID == m.id {
				log.Infof(ctx, "Leader elected: %v", msg.LeaderID)
				go m.lead(ctx, halt)
			} else {
				log.Infof(ctx, "Leader observed: %v:%v", msg.LeaderID, msg.LeaderAddr)
				go m.follow(ctx, halt, msg.LeaderID, msg.LeaderAddr)
			}

		case <-m.Closed():
			return
		}
	}
}

func (m *LeaderManager) lead(ctx context.Context, halt iox.AsyncCloser) {
	wctx, cancel := contextx.WithQuitCancel(ctx, halt.Closed())
	defer cancel()

	for {
		m.mu.Lock()
		if halt.IsClosed() {
			m.mu.Unlock()
			return // leadership change
		}

		// (1) Create new leader and wire it in if not halted

		l, proxy := m.factory(wctx)
		m.local = proxy
		m.mu.Unlock()

		// (2) Wait for termination. It may be self-initiated, which is unexpected.
		// In that case, it is re-created.

		<-l.Closed()
		if halt.IsClosed() {
			return
		}

		log.Errorf(ctx, "Leader %v closed unexpectedly", l)
		m.cl.Sleep(time.Second)
	}
}

func (m *LeaderManager) follow(ctx context.Context, halt iox.AsyncCloser, id raft.ServerID, addr raft.ServerAddress) {
	for !halt.IsClosed() {
		// (1) Dial leader. If it (unexpectedly) fails, re-try until leader change.

		cc, err := grpcx.DialNonBlocking(ctx, string(addr), grpcx.WithInsecure())
		if err != nil {
			log.Errorf(ctx, "Failed to dial leader %v:%v: %v", id, addr, err)
			time.Sleep(4 * time.Second)
			continue
		}

		log.Debugf(ctx, "Connected to remote leader %v:%v", id, addr)

		// (2) Once connected, wire in.

		m.mu.Lock()
		if halt.IsClosed() {
			m.mu.Unlock()
			return // leadership change
		}
		m.remote = internal_v1.NewLeaderServiceClient(cc)
		m.mu.Unlock()

		// TODO(herohde) 9/7/2023: periodic connection health check at the grpc level?

		<-halt.Closed()
	}
}

func (m *LeaderManager) tryRemote() (internal_v1.LeaderServiceClient, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.remote, m.remote != nil
}

func (m *LeaderManager) tryLocal() (leader.Proxy, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.local, m.local != nil
}

func (m *LeaderManager) reset() iox.AsyncCloser {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.halt != nil {
		m.halt.Close()
	}
	m.local = nil
	m.remote = nil
	m.halt = iox.NewAsyncCloser()
	return m.halt
}
