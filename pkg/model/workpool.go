package model

import (
	"context"
	"atoms.co/lib-go/pkg/clock"
	"go.atoms.co/splitter/lib/service/session"
	"go.atoms.co/lib/log"
	"go.atoms.co/lib/chanx"
	"go.atoms.co/lib/clockx"
	"go.atoms.co/lib/net/grpcx"
	"go.atoms.co/lib/iox"
	"go.atoms.co/lib/mapx"
	"go.atoms.co/lib/syncx"
	"go.atoms.co/splitter/pb"
	"fmt"
	"go.uber.org/atomic"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"sync"
	"time"
)

const (
	drainWaitTime = 30 * time.Second
)

type JoinFn func(context.Context, ...grpc.CallOption) (public_v1.ConsumerService_JoinClient, error)

// WorkPool manages a pool of grants assigned by Splitter to the consumer and provides updated clusters. It maintains
// connection with the service, re-connecting on failures, and continues to manage assigned grants when disconnected.
//
// Life cycle is controlled by the context passed on creation; context cancellation triggers draining procedure.
// `Drained()` can be used to detect when draining has stopped and the pool is being closed.
// `Closed()` can be used to detect when draining has stopped and the pool is closed. The pool can close itself when
// the lease is expired, and it wasn't renewed by the service for some reason.
type WorkPool struct {
	iox.AsyncCloser
	drain iox.AsyncCloser

	cl clock.Clock

	inject        chan func()
	expiredGrants chan GrantID // some grants possibly expired or closed
	expiredLease  chan bool

	state *consumerState
	con   *connection
}

func NewWorkPool(ctx context.Context, cl clock.Clock, consumer Consumer, service QualifiedServiceName, domains []QualifiedDomainName, joinFn JoinFn, handlerFn Handler) (*WorkPool, <-chan Cluster) {
	ctx = consumerCtx(ctx, consumer, service)

	state, expiredLease, expiredGrants := newConsumerState(cl, consumer, service, domains, handlerFn)

	closer := iox.NewAsyncCloser()
	p := &WorkPool{
		AsyncCloser: closer,
		drain:       iox.WithQuit(closer.Closed(), iox.WithCancel(ctx, iox.NewAsyncCloser())),
		cl:          cl,

		inject:        make(chan func()),
		expiredGrants: expiredGrants,
		expiredLease:  expiredLease,

		state: state,
	}

	var cluster <-chan Cluster
	p.con, cluster = newConnection(cl, p.txn, joinFn, state)

	go p.process(consumerCtx(context.Background(), consumer, service))

	log.Infof(ctx, "Initialized work pool: %v", p.state.Consumer())
	return p, cluster
}

// Drained returns a channel that is closed when the pool is drained.
func (p *WorkPool) Drained() <-chan struct{} {
	return p.drain.Closed()
}

// process performs processing in the main thread
func (p *WorkPool) process(ctx context.Context) {
	defer p.Close()
	defer p.state.Close()
	defer p.con.Close()

	for {
		select {
		case fn := <-p.inject:
			fn()

		// Expired grants must be handled even if connection is not present and must not block the main thread
		case gid := <-p.expiredGrants:
			if p.state.RemoveIfExpired(ctx, gid) {
				go func() {
					p.con.Send(NewConsumerReleasedMessage(NewReleasedMessage(gid)))
				}()
			}

		case <-p.expiredLease:
			if p.state.IsLeaseExpired() {
				// Consumer lost the lease, most probably due to disconnect from the service and lack of lease extensions.
				// All leases assigned to this consumer are considered expired automatically.
				log.Errorf(ctx, "Work pool lease %v expired at %v", p.state.Lease(), p.cl.Now())
				return
			}

		case <-p.drain.Closed():
			p.state.Drain(drainWaitTime)
			return

		case <-p.Closed():
			return
		}
	}
}

// txn runs the given function in the main thread sync. Any signal that triggers a complex action must
// perform I/O or expensive parts outside txn and potentially use multiple txn calls.
func (p *WorkPool) txn(ctx context.Context, fn func() error) error {
	var wg sync.WaitGroup
	var err error

	wg.Add(1)
	select {
	case p.inject <- func() {
		defer wg.Done()
		err = fn()
	}:
		wg.Wait()
		return err
	case <-p.Closed():
		return ErrDraining
	case <-ctx.Done():
		return ErrDraining
	}
}

// consumerState manages the consumer state, including the consumer lease and all the grants. Non-thread safe.
// Life cycle is controlled by `Drain` to drain all the grants and to close the state and grants; or by `Close()`
// to close the state and grants without draining.
type consumerState struct {
	iox.AsyncCloser

	clock        clock.Clock
	consumer     Consumer
	service      QualifiedServiceName
	domains      []QualifiedDomainName
	leaseExpired chan bool
	grantExpired chan GrantID
	handlerFn    Handler

	grants      map[GrantID]*grantInfo
	assignments map[ConsumerID]map[GrantID]Grant
	lease       time.Time
	leaseTimer  *clockx.Timer
}

func newConsumerState(cl clock.Clock, consumer Consumer, service QualifiedServiceName, domains []QualifiedDomainName, handlerFn Handler) (state *consumerState, expiredLease chan bool, expiredGrants chan GrantID) {
	expiredLease = make(chan bool, 1)
	expiredGrants = make(chan GrantID, 100)
	s := &consumerState{
		AsyncCloser: iox.NewAsyncCloser(),

		clock:        cl,
		consumer:     consumer,
		service:      service,
		domains:      domains,
		leaseExpired: expiredLease,
		grantExpired: expiredGrants,
		handlerFn:    handlerFn,

		grants:      map[GrantID]*grantInfo{},
		assignments: map[ConsumerID]map[GrantID]Grant{},
	}
	return s, expiredLease, expiredGrants
}

func (s *consumerState) Consumer() Consumer {
	return s.consumer
}

func (s *consumerState) Service() QualifiedServiceName {
	return s.service
}

func (s *consumerState) Domains() []QualifiedDomainName {
	return s.domains
}

func (s *consumerState) Grants() []Grant {
	return mapx.MapValues(s.grants, func(g *grantInfo) Grant {
		return g.Grant()
	})
}

func (s *consumerState) GrantsCount() int {
	return len(s.grants)
}

func (s *consumerState) Lease() time.Time {
	return s.lease
}

func (s *consumerState) StaleActiveGrants() {
	for _, info := range s.grants {
		info.Stale()
	}
}

func (s *consumerState) SetAssignments(ctx context.Context, assignments []Assignment) Cluster {
	fresh := map[ConsumerID]map[GrantID]Grant{}
	for _, assignment := range assignments {
		consumerGrants, err := assignment.ParseGrants()
		if err != nil {
			log.Errorf(ctx, "Unable to parse grants in cluster snapshot, skipping: %v", err)
			continue
		}
		grants := map[GrantID]Grant{}
		for _, grant := range consumerGrants {
			grants[grant.ID] = grant
		}
		fresh[assignment.Consumer().ID()] = grants
	}
	s.assignments = fresh
	return Cluster{}
}

func (s *consumerState) UpdateAssignments(ctx context.Context, assignments []Assignment, removed []GrantID) Cluster {
	var allGrants map[ConsumerID]map[GrantID]Grant
	for _, assignment := range assignments {
		consumerGrants, err := assignment.ParseGrants()
		if err != nil {
			log.Errorf(ctx, "Unable to parse grants in cluster update, skipping: %v", err)
			continue
		}
		grants := map[GrantID]Grant{}
		for _, grant := range consumerGrants {
			grants[grant.ID] = grant
		}
		allGrants[assignment.Consumer().ID()] = grants
	}

	for cid, grants := range allGrants {
		s.assignments[cid] = grants
	}
	for _, r := range removed {
		for _, grants := range s.assignments {
			if _, ok := grants[r]; ok {
				delete(grants, r)
			}
		}
	}
	return Cluster{}
}

func (s *consumerState) ExtendLease(lease time.Time) {
	if lease.Before(s.lease) {
		return
	}

	s.lease = lease
	ttl := lease.Sub(s.clock.Now())

	if s.leaseTimer == nil {
		s.leaseTimer = clockx.NewTimer(s.clock, ttl)
		go func() {
			for {
				select {
				case <-s.Closed():
					return
				case <-s.leaseTimer.C:
					chanx.Clear(s.leaseExpired)
					s.leaseExpired <- true
				}
			}
		}()
	} else {
		s.leaseTimer.Reset(ttl)
	}

	for _, info := range s.grants {
		if info.state != active || info.IsClosed() {
			continue // skip: don't extend inactive or closed allocations
		}
		info.UpdateLease(lease)
	}
}

func (s *consumerState) IsLeaseExpired() bool {
	return s.lease.Before(s.clock.Now())
}

func (s *consumerState) RevokeGrants(ctx context.Context, grants []GrantID) {
	for _, grantId := range grants {
		info, ok := s.grants[grantId]
		if !ok {
			log.Errorf(ctx, "Leader revoked unknown grant %v. Ignoring", grantId)
			continue
		}
		info.Drain()
	}
}

func (s *consumerState) RemoveIfExpired(ctx context.Context, gid GrantID) bool {
	info, ok := s.grants[gid]
	if !ok {
		log.Errorf(ctx, "Unable to find grant %v when checking its expiration", gid)
		return false
	}
	now := s.clock.Now()

	if !info.IsClosed() && info.Grant().Lease.After(now) {
		return false // live
	}

	log.Warnf(ctx, "Grant %v expired or closed", info)
	delete(s.grants, gid)
	return true
}

func (s *consumerState) AssignGrants(ctx context.Context, grants []Grant) {
	for _, grant := range grants {
		s.assignGrant(ctx, grant)
	}
}

func (s *consumerState) Drain(duration time.Duration) {
	for _, info := range s.grants {
		info.Drain()
	}

	s.clock.AfterFunc(duration, s.closeGrants)

	for _, info := range s.grants {
		<-info.Closed()
	}

	s.AsyncCloser.Close()
}

func (s *consumerState) Close() {
	if s.AsyncCloser.IsClosed() {
		return
	}
	s.closeGrants()
	s.AsyncCloser.Close()
}

func (s *consumerState) closeGrants() {
	for _, info := range s.grants {
		info.Close()
	}
}

func (s *consumerState) assignGrant(ctx context.Context, grant Grant) {
	// Determine if the grant is a continuation for an existing stale grant as a result of a server change.
	// We then simply revive it and do nothing.

	if grant.Lease.After(s.lease) {
		grant.Lease = s.lease
	}

	if info, ok := s.grants[grant.ID]; ok && !info.IsClosed() {
		switch info.State() {
		case stale:
			log.Infof(ctx, "New grant will activate stale grant %v", info.Grant())
			info.UpdateLease(grant.Lease)
			return

		case active:
			log.Errorf(ctx, "New grant %v duplicates existing active grant %v. Ignoring", grant, info.Grant())
			return

		default:
			// If disconnecting, we would have to wait out the drain before creating a new shard.
			// Unsupported and not an expected grant from the server. Close shard, for now.

			log.Errorf(ctx, "Invalid grant state %v for new grant %v. Closing", info.State(), info.Grant())
			info.Close()
		}
	}

	s.grants[grant.ID] = newGrantInfo(s.clock, s.handlerFn, s.grantExpired, grant)
}

// consumerGrantState represents the current state of a shard tracked by the work pool. A shard is stale when
// disconnected with a valid lease.
type consumerGrantState string

const (
	active   consumerGrantState = "active"
	draining                    = "draining"
	stale                       = "stale"
)

type grantInfo struct {
	iox.AsyncCloser
	drainer iox.AsyncCloser

	cl    clock.Clock
	grant Grant
	state consumerGrantState
	lease *session.ExtendableLease
	timer *clockx.Timer
}

func newGrantInfo(cl clock.Clock, handlerFn Handler, grantExpired chan<- GrantID, grant Grant) *grantInfo {
	closer := iox.NewAsyncCloser()
	drainer := iox.NewAsyncCloser()

	ttl := grant.Lease.Sub(cl.Now())
	info := &grantInfo{
		AsyncCloser: closer,
		cl:          cl,
		grant:       grant,
		drainer:     drainer,
		state:       active,
		lease:       session.NewExtendableLease(drainer, grant.Lease),
		timer:       clockx.NewTimer(cl, ttl),
	}

	startHandler(handlerFn, grant, closer, info.lease)

	go func() {
		for {
			select {
			case <-closer.Closed():
				grantExpired <- grant.ID
				return
			case <-info.timer.C:
				// This may race with timer reset, but should be no-op if lease is extended
				grantExpired <- grant.ID
				// Exit only when it's closed.
			}
		}
	}()
	return info
}

func (g *grantInfo) Grant() Grant {
	return g.grant
}

func (g *grantInfo) State() consumerGrantState {
	return g.state
}

func (g *grantInfo) UpdateLease(lease time.Time) {
	g.state = active
	if g.grant.Lease.After(lease) {
		return
	}
	g.grant.Lease = lease
	g.lease.Extend(lease)
	g.resetTimer()
}

func (g *grantInfo) Stale() {
	if g.state == active {
		g.state = stale
	}
}

func (g *grantInfo) Drain() {
	g.state = draining
	g.drainer.Closed()
}

func (g *grantInfo) resetTimer() {
	ttl := g.grant.Lease.Sub(g.cl.Now())
	g.timer.Reset(ttl)
}

func startHandler(handler Handler, grant Grant, closer iox.AsyncCloser, lease Lease) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		defer closer.Close()
		handler(ctx, grant.Shard, lease)
	}()

	go func() {
		defer cancel()
		select {
		case <-closer.Closed():
		}
	}()
}

// connection maintains communication with the service, handles incoming messages by acting on the consumer state
// and sends complete clusters on all changes from the service (snapshot and incremental updates).
// Life cycle is controlled by `Close()` which will stop all communication and drop all the inflight messages. Must
// be used only when work pool is drained or its lease is expired.
type connection struct {
	iox.AsyncCloser

	clock  clock.Clock
	txn    syncx.TxnFn
	joinFn JoinFn

	state     *consumerState
	connected atomic.Bool
	cluster   chan Cluster
	out       chan ConsumerMessage
}

func newConnection(cl clock.Clock, txn syncx.TxnFn, joinFn JoinFn, state *consumerState) (*connection, <-chan Cluster) {
	cluster := make(chan Cluster, 100)
	ctx := consumerCtx(context.Background(), state.Consumer(), state.Service())
	c := &connection{
		AsyncCloser: iox.WithQuit(ctx.Done(), iox.NewAsyncCloser()),
		clock:       cl,
		txn:         txn,
		joinFn:      joinFn,
		state:       state,
		cluster:     cluster,
		out:         make(chan ConsumerMessage, 1000),
	}
	go c.process(ctx)
	return c, cluster
}

func (c *connection) Send(msg ConsumerMessage) {
	if !c.connected.Load() {
		return
	}

	select {
	case c.out <- msg:
	case <-c.Closed():
	}
}

// connect maintains communication with the server, off the main thread. All operations must be coordinated
// using txn.
func (c *connection) process(ctx context.Context) {
	var err error
	limiter := rate.NewLimiter(rate.Every(time.Minute), 3)
	for {
		if c.IsClosed() {
			return
		}

		err = limiter.Wait(ctx)
		if err != nil {
			c.clock.Sleep(time.Second * 30)
			continue
		}

		now := c.clock.Now()
		log.Infof(ctx, "Attempting to connect to Splitter at %v", now)

		grants, err := syncx.Txn1(ctx, c.txn, func() ([]Grant, error) {
			return c.state.Grants(), nil
		})
		if err != nil {
			log.Infof(ctx, "%v closed before connecting to Splitter at %v", c, now)
			return
		}

		// TODO (styurin, 10/23/2023): handle draining grants

		err = c.join(ctx, grants)
		if err != nil {
			log.Errorf(ctx, "Failed to join Splitter: %v", err)
		}

		syncx.Txn0(ctx, c.txn, c.state.StaleActiveGrants)
	}
}

func (c *connection) join(ctx context.Context, grants []Grant) error {
	// Create a client session with the worker instance
	sess, establish, sessionOut := session.NewClient(ctx, c.clock, c.state.Consumer().Client())
	go func() {
		select {
		case <-sess.Closed():
			c.connected.Store(false)
		}
	}()

	return grpcx.Connect(ctx, c.joinFn, func(ctx context.Context, coordinatorIn <-chan *public_v1.ConsumerMessage) (<-chan *public_v1.ConsumerMessage, error) {
		// Process incoming messages by either copying to the output buffer or sending to the session
		in := chanx.MapIf(coordinatorIn, func(pb *public_v1.ConsumerMessage) (ConsumerMessage, bool) {
			if pb.GetSession() != nil {
				sess.Observe(ctx, session.WrapMessage(pb.GetSession()))
				// Do not propagate session messages to the client
				return ConsumerMessage{}, false
			}
			return WrapConsumerMessage(pb), true
		})

		chanx.Clear(c.out)
		c.connected.Store(true)
		go c.processSession(ctx, grants, in)

		// Send register as the first message
		register := NewRegisterMessage(c.state.Consumer(), c.state.Service(), c.state.Domains(), grants)
		out := chanx.Envelope(NewConsumerRegisterMessage(register), c.out, NewConsumerDeregisterMessage())

		joined := session.Connect(sess, establish, out, sessionOut, NewConsumerSessionMessage)
		return chanx.Map(joined, UnwrapConsumerMessage), nil
	})
}

func (c *connection) processSession(ctx context.Context, grants []Grant, in <-chan ConsumerMessage) {
	now := c.clock.Now()

	// Process messages from splitter and clients as well as allocation expiration events. The splitter guarantees to send
	// state, cluster and lease updates as the first messages.

	log.Infof(ctx, "Connected to Splitter with %v existing allocation(s) at %v", len(grants), now)

	defer func() {
		log.Infof(ctx, "Disconnected from Splitter after %v", c.clock.Since(now))
	}()

	for {
		// Exit abruptly when closed. Connection is closed only when work pool is drained and no communication with
		// service is expected.
		if c.IsClosed() {
			log.Infof(ctx, "Connection %v closed", c)
			return
		}

		select {
		case msg, ok := <-in:
			if !ok {
				log.Warnf(ctx, "Unexpected closed connection")
				return
			}
			c.handleConsumerMessage(ctx, msg)

		case <-c.Closed():
			log.Infof(ctx, "Connection %v closed", c)
			return
		}
	}
}

func (c *connection) handleConsumerMessage(ctx context.Context, msg ConsumerMessage) {
	switch {

	case msg.IsCluster():
		// Cluster update. Merge with current cluster and forward to listeners.
		cluster, _ := msg.GetCluster()
		switch {
		case cluster.IsSnapshot():
			m, _ := cluster.Snapshot()
			c.handleClusterSnapshot(ctx, m)
		case cluster.IsUpdate():
			m, _ := cluster.Update()
			c.handleClusterUpdate(ctx, m)
		default:
			log.Errorf(ctx, "Unexpected cluster message: %v", cluster)
		}

	case msg.IsExtend():
		// Worker lease update. The new expiration time applies to all "active" grants.
		m, _ := msg.GetExtend()
		syncx.Txn0(ctx, c.txn, func() {
			c.state.ExtendLease(m.GetLease())
		})

	case msg.IsAssign():
		m, _ := msg.GetAssign()
		c.handleAssignedGrants(ctx, m)

	case msg.IsRevoke():
		m, _ := msg.GetRevoke()
		c.handleRevokedGrants(ctx, m)

	default:
		log.Errorf(ctx, "Unexpected coordinator message: %v", msg)
	}
}

func (c *connection) handleClusterSnapshot(ctx context.Context, snapshot ClusterSnapshot) {
	log.Debugf(ctx, "Storing initial assignments from cluster snapshot: %v", snapshot)
	cluster, err := syncx.Txn1(ctx, c.txn, func() (Cluster, error) {
		return c.state.SetAssignments(ctx, snapshot.Assignments()), nil
	})
	if err != nil {
		log.Warnf(ctx, "Connection %v closed while processing cluster snapshot", c)
		return
	}
	chanx.Clear(c.out)
	c.cluster <- cluster
}

func (c *connection) handleClusterUpdate(ctx context.Context, update ClusterUpdate) {
	log.Debugf(ctx, "Updating assignments from cluster update: %v", update)
	removed := update.Removed()
	cluster, err := syncx.Txn1(ctx, c.txn, func() (Cluster, error) {
		return c.state.UpdateAssignments(ctx, update.Assignments(), removed), nil
	})
	if err != nil {
		log.Warnf(ctx, "Connection %v closed while processing cluster update", c)
		return
	}
	chanx.Clear(c.out)
	c.cluster <- cluster
}

func (c *connection) handleAssignedGrants(ctx context.Context, assigned AssignMessage) {
	grants := assigned.Grants()

	log.Infof(ctx, "Received assigned grants: %v", grants)

	syncx.Txn0(ctx, c.txn, func() {
		c.state.AssignGrants(ctx, grants)
	})
}

func (c *connection) handleRevokedGrants(ctx context.Context, revoked RevokeMessage) {
	grants := revoked.Grants()

	log.Infof(ctx, "Received revoked grants: %v", grants)

	syncx.Txn0(ctx, c.txn, func() {
		c.state.RevokeGrants(ctx, grants)
	})
}

func (c *connection) String() string {
	return fmt.Sprintf("connection{service=%v,consumer=%v}", c.state.Service(), c.state.Consumer())
}
