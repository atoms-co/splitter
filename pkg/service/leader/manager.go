package leader

import (
	"context"
	"atoms.co/lib-go/pkg/clock"
	"go.atoms.co/splitter/lib/service/session"
	"go.atoms.co/lib/log"
	"go.atoms.co/lib/contextx"
	"go.atoms.co/lib/net/grpcx"
	"go.atoms.co/lib/iox"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pb/private"
	"fmt"
	"sync"
	"time"
)

type DirectiveType string

const (
	Lead       DirectiveType = "lead"
	Follow     DirectiveType = "follow"
	Disconnect DirectiveType = "disconnect"
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
type Manager struct {
	iox.AsyncCloser

	cl      clock.Clock
	factory Factory

	remote internal_v1.LeaderServiceClient // nil if local
	local  Proxy                           // nil if remote
	halt   iox.AsyncCloser                 // stop signal for local/remote re-creation if active
	mu     sync.Mutex
}

func NewManager(cl clock.Clock, in <-chan Directive, factory Factory) *Manager {
	ret := &Manager{
		AsyncCloser: iox.NewAsyncCloser(),
		cl:          cl,
		factory:     factory,
	}
	go ret.process(context.Background(), in)

	return ret
}

func (m *Manager) Resolve(ctx context.Context, key model.DomainKey) (internal_v1.LeaderServiceClient, error) {
	if client, ok := m.tryRemote(); ok {
		return client, nil
	}
	return nil, model.ErrNoResolution
}

func (m *Manager) Join(ctx context.Context, sid session.ID, id model.Instance, grants []Grant, in <-chan Message) (<-chan Message, error) {
	if l, ok := m.tryLocal(); ok {
		return l.Join(ctx, sid, id, grants, in)
	}
	return nil, model.ErrNotOwned
}

func (m *Manager) Handle(ctx context.Context, request HandleRequest) (*internal_v1.LeaderHandleResponse, error) {
	if l, ok := m.tryLocal(); ok {
		return l.Handle(ctx, request)
	}
	return nil, model.ErrNotOwned
}

func (m *Manager) process(ctx context.Context, in <-chan Directive) {
	defer m.Close()
	defer m.reset()

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

		case <-m.Closed():
			return
		}
	}
}

func (m *Manager) lead(ctx context.Context, halt iox.AsyncCloser, id string) {
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

func (m *Manager) follow(ctx context.Context, halt iox.AsyncCloser, id, endpoint string) {
	for !halt.IsClosed() {
		// (1) Dial leader. If it (unexpectedly) fails, re-try until leader change.

		cc, err := grpcx.DialNonBlocking(ctx, endpoint, grpcx.WithInsecure())
		if err != nil {
			log.Errorf(ctx, "Failed to dial leader %v @%v: %v", id, endpoint, err)
			time.Sleep(4 * time.Second)
			continue
		}

		log.Debugf(ctx, "Connected to remote leader %v @%v", id, endpoint)

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

func (m *Manager) tryRemote() (internal_v1.LeaderServiceClient, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.remote, m.remote != nil
}

func (m *Manager) tryLocal() (Proxy, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.local, m.local != nil
}

func (m *Manager) reset() iox.AsyncCloser {
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
