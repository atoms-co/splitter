package leader

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.atoms.co/splitter/lib/service/session"
	"go.atoms.co/lib/log"
	"go.atoms.co/lib/contextx"
	"go.atoms.co/lib/net/grpcx"
	"go.atoms.co/lib/iox"
	"go.atoms.co/splitter/pkg/model"
	splitterprivatepb "go.atoms.co/splitter/pb/private"
)

type DirectiveType string

const (
	Lead       DirectiveType = "lead"
	Follow     DirectiveType = "follow"
	Disconnect DirectiveType = "disconnect"

	leaderDrainTime = 5 * time.Second
)

// Directive is a leadership directive used by the manager.
type Directive struct {
	Type     DirectiveType
	ID       string
	Endpoint string // if follow
}

func (d Directive) String() string {
	switch d.Type {
	case Lead:
		return fmt.Sprintf("%v[%v]", d.Type, d.ID)
	case Disconnect:
		return fmt.Sprintf("%v", d.Type)
	default:
		return fmt.Sprintf("%v[%v @%v]", d.Type, d.ID, d.Endpoint)
	}
}

type Factory func(ctx context.Context) (iox.AsyncCloser, Proxy)

// Manager handles a proxy lifecycle and remote resolution. It is controlled by a stream
// of leadership change directives.
type Manager interface {
	iox.RAsyncCloser

	Resolve(ctx context.Context, key model.DomainKey) (splitterprivatepb.LeaderServiceClient, error)
	Join(ctx context.Context, sid session.ID, in <-chan Message) (<-chan Message, error)
	Handle(ctx context.Context, request HandleRequest) (*splitterprivatepb.LeaderHandleResponse, error)
	Drain(timeout time.Duration)
}

type ResolvingManager struct {
	iox.AsyncCloser

	factory Factory

	remote splitterprivatepb.LeaderServiceClient // nil if local
	local  Proxy                              // nil if remote
	halt   iox.AsyncCloser                    // stop signal for local/remote re-creation if active
	mu     sync.Mutex

	drain iox.AsyncCloser
}

func NewManager(in <-chan Directive, factory Factory) *ResolvingManager {
	quit := iox.NewAsyncCloser()
	ret := &ResolvingManager{
		AsyncCloser: quit,
		factory:     factory,
		drain:       iox.WithQuit(quit.Closed(), iox.NewAsyncCloser()),
	}
	go ret.process(context.Background(), in)

	return ret
}

func (m *ResolvingManager) Drain(timeout time.Duration) {
	m.drain.Close()
	time.AfterFunc(timeout, m.Close)
}

func (m *ResolvingManager) Resolve(ctx context.Context, key model.DomainKey) (splitterprivatepb.LeaderServiceClient, error) {
	if client, ok := m.tryRemote(); ok {
		return client, nil
	}
	return nil, model.ErrNoResolution
}

func (m *ResolvingManager) Join(ctx context.Context, sid session.ID, in <-chan Message) (<-chan Message, error) {
	if l, ok := m.tryLocal(); ok {
		return l.Join(ctx, sid, in)
	}
	return nil, model.ErrNotOwned
}

func (m *ResolvingManager) Handle(ctx context.Context, request HandleRequest) (*splitterprivatepb.LeaderHandleResponse, error) {
	if l, ok := m.tryLocal(); ok {
		return l.Handle(ctx, request)
	}
	return nil, model.ErrNotOwned
}

func (m *ResolvingManager) process(ctx context.Context, in <-chan Directive) {
	defer m.Close()
	defer m.reset()

steady:
	for {
		select {
		case directive, ok := <-in:
			if !ok {
				return
			}
			halt := m.reset()

			// On leader change, async create or establish connection to leader. As long as
			// the leader has not changed again, re-creation may be necessary. The 'halt'
			// signal delimits that.

			switch directive.Type {
			case Lead:
				log.Infof(ctx, "Leader elected: %v", directive.ID)
				go m.lead(ctx, halt, directive.ID)

			case Follow:
				log.Infof(ctx, "Leader observed: %v @%v", directive.ID, directive.Endpoint)
				go m.follow(ctx, halt, directive.ID, directive.Endpoint)

			case Disconnect:
				log.Infof(ctx, "Leader disconnected")

			default:
				log.Errorf(ctx, "Internal: invalid directive type: %v", directive)
			}

		case <-m.drain.Closed():
			break steady

		case <-m.Closed():
			return
		}
	}

	log.Infof(ctx, "Leader Manager draining")

	m.mu.Lock()
	isLeader := m.local != nil
	m.mu.Unlock()

	// Gives time to reassign Coordinators to other nodes
	if isLeader {
		time.Sleep(leaderDrainTime)
	}
}

func (m *ResolvingManager) lead(ctx context.Context, halt iox.AsyncCloser, id string) {
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
		time.Sleep(time.Second)
	}
}

func (m *ResolvingManager) follow(ctx context.Context, halt iox.AsyncCloser, id, endpoint string) {
	for !halt.IsClosed() {
		// (1) Dial leader. If it (unexpectedly) fails, re-try until leader change.

		cc, err := grpcx.DialNonBlocking(ctx, endpoint, grpcx.WithInsecure())
		if err != nil {
			log.Errorf(ctx, "Failed to dial leader %v @%v: %v", id, endpoint, err)
			time.Sleep(4 * time.Second)
			continue
		}

		log.Infof(ctx, "Connected to remote leader %v @%v", id, endpoint)

		// (2) Once connected, wire in.

		m.mu.Lock()
		if halt.IsClosed() {
			m.mu.Unlock()
			return // leadership change
		}
		m.remote = splitterprivatepb.NewLeaderServiceClient(cc)
		m.mu.Unlock()

		// TODO(herohde) 9/7/2023: periodic connection health check at the grpc level?

		<-halt.Closed()
	}
}

func (m *ResolvingManager) tryRemote() (splitterprivatepb.LeaderServiceClient, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.remote, m.remote != nil
}

func (m *ResolvingManager) tryLocal() (Proxy, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.local, m.local != nil
}

func (m *ResolvingManager) reset() iox.AsyncCloser {
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
