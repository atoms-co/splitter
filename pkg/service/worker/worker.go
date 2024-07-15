package worker

import (
	"context"
	"atoms.co/lib-go/pkg/clock"
	"go.atoms.co/splitter/lib/service/location"
	"go.atoms.co/splitter/lib/service/session"
	"go.atoms.co/lib/log"
	"go.atoms.co/lib/metrics"
	"go.atoms.co/lib/chanx"
	"go.atoms.co/lib/clockx"
	"go.atoms.co/lib/contextx"
	"go.atoms.co/lib/net/grpcx"
	"go.atoms.co/lib/iox"
	"go.atoms.co/lib/mapx"
	"go.atoms.co/lib/randx"
	"go.atoms.co/slicex"
	"go.atoms.co/lib/syncx"
	"go.atoms.co/splitter/pkg/core"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pkg/service/coordinator"
	"go.atoms.co/splitter/pkg/service/leader"
	"go.atoms.co/splitter/pb/private"
	"fmt"
	"sync"
	"time"
)

const (
	statsDuration             = 15 * time.Second
	coordinatorStateBufferLen = 20
)

var (
	numGrants = metrics.NewTrackedGauge(metrics.NewGauge("go.atoms.co/splitter/worker_grants", "Worker grants", slicex.CopyAppend(core.QualifiedServiceKeys, core.LeaseStateKey)...))
)

type JoinFn func(ctx context.Context, self location.Instance, handler grpcx.Handler[leader.Message, leader.Message]) error

type CoordinatorFactory func(ctx context.Context, service model.QualifiedServiceName, state core.State, updates <-chan core.Update) coordinator.Coordinator

type joinStatus struct {
	connected time.Time
}

// Worker manages coordinators and emits internal cluster updates from the leader.
type Worker struct {
	iox.AsyncCloser

	cl      clock.Clock
	self    model.Instance
	joinFn  JoinFn
	factory CoordinatorFactory

	status *joinStatus           // leader connectivity status
	in     <-chan leader.Message // leader incoming messages (empty and not closed, if disconnected)
	out    chan<- leader.Message // leader outgoing messages (empty and not closed, if disconnected)

	cluster  *core.Cluster
	clusters chan *core.Cluster

	grants   map[core.GrantID]*Grant
	services map[model.QualifiedServiceName]core.GrantID
	lease    *clockx.Timer

	expire chan bool
	inject chan func()

	drain iox.AsyncCloser
}

func New(cl clock.Clock, loc location.Location, endpoint string, joinFn JoinFn, factory CoordinatorFactory) (*Worker, <-chan *core.Cluster) {
	quit := iox.NewAsyncCloser()
	w := &Worker{
		AsyncCloser: quit,
		cl:          cl,
		self:        model.NewInstance(location.NewNamedInstance("worker", loc), endpoint),
		joinFn:      joinFn,
		factory:     factory,
		in:          make(chan leader.Message),
		out:         make(chan leader.Message),
		clusters:    make(chan *core.Cluster, 1),
		grants:      map[core.GrantID]*Grant{},
		services:    map[model.QualifiedServiceName]core.GrantID{},
		expire:      make(chan bool, 1),
		inject:      make(chan func()),
		drain:       iox.WithQuit(quit.Closed(), iox.NewAsyncCloser()),
	}
	go w.join(context.Background())
	go w.process(context.Background())
	return w, w.clusters
}

func (w *Worker) Self() model.Instance {
	return w.self
}

// Connect handles connection of a consumer to a local coordinator
func (w *Worker) Connect(ctx context.Context, sid session.ID, in <-chan model.ConsumerMessage) (<-chan model.ConsumerMessage, error) {
	msg, ok := chanx.TryRead(in, 20*time.Second)
	if !ok {
		log.Errorf(ctx, "No registration message received: %v", msg)
		return nil, fmt.Errorf("no registration message received %v: %w", msg, model.ErrInvalid)
	}

	clientMsg, ok := msg.ClientMessage()
	if !ok || !clientMsg.IsRegister() {
		log.Errorf(ctx, "expected registration message, got %v", clientMsg)
		return nil, model.WrapError(fmt.Errorf("invalid registration message: %w", model.ErrInvalid))
	}
	register, _ := clientMsg.Register()

	service := register.Service()
	c, err := syncx.Txn1(ctx, w.txn, func() (coordinator.Coordinator, error) {
		gid, ok := w.services[service]
		if !ok {
			return nil, fmt.Errorf("service not found %v: %w", service, model.ErrNotOwned)
		}
		return w.grants[gid].Coordinator, nil
	})
	if err != nil {
		return nil, err
	}
	return c.Connect(ctx, sid, chanx.Prepend(in, msg))
}

func (w *Worker) Handle(ctx context.Context, req coordinator.HandleRequest) (*internal_v1.CoordinatorHandleResponse, error) {
	c, err := syncx.Txn1(ctx, w.txn, func() (coordinator.Coordinator, error) {
		gid, ok := w.services[req.Service()]
		if !ok {
			return nil, fmt.Errorf("service %v not found: %w", req.Service(), model.ErrNotOwned)
		}
		return w.grants[gid].Coordinator, nil
	})
	if err != nil {
		return nil, err
	}
	return c.Handle(ctx, req)
}

func (w *Worker) Drain(timeout time.Duration) {
	w.drain.Close()
	w.cl.AfterFunc(timeout, w.Close)
}

func (w *Worker) join(ctx context.Context) {
	wctx, _ := contextx.WithQuitCancel(ctx, w.Closed())
	for !w.drain.IsClosed() {
		// Not connected. Establish connection with leader with blocking join call. The worker is either
		// connecting or connected while in the join call.

		log.Infof(ctx, "Worker %v attempting to join leader", w.self)

		err := w.joinFn(wctx, w.self.Instance(), func(ctx context.Context, in <-chan leader.Message) (<-chan leader.Message, error) {
			return syncx.Txn1(ctx, w.txn, func() (<-chan leader.Message, error) {
				return w.joinLeader(ctx, in)
			})
		})

		log.Infof(ctx, "Worker %v disconnected from leader: %v", w.self, err)

		syncx.Txn0(wctx, w.txn, func() {
			w.lostLeader(ctx)
		})

		w.cl.Sleep(time.Second + randx.Duration(time.Second)) // short random backoff on disconnect
	}
}

func (w *Worker) joinLeader(ctx context.Context, in <-chan leader.Message) (<-chan leader.Message, error) {
	w.lostLeader(ctx) // sanity check

	active := mapx.MapValuesIf(w.grants, func(g *Grant) (core.Grant, bool) {
		return g.ToUpdated(), g.State == LeaseStale
	})

	log.Infof(ctx, "Connected to leader, #grants=%v, #active=%v", len(w.grants), len(active))

	out := make(chan leader.Message, 2_000)
	out <- leader.NewRegister(w.self, active...)

	w.in = in
	w.out = out
	w.status = &joinStatus{
		connected: w.cl.Now(),
	}
	w.lease = nil

	return out, nil
}

func (w *Worker) lostLeader(ctx context.Context) {
	if w.status == nil {
		return // ok: already disconnected
	}

	log.Infof(ctx, "Lost connection to leader, connected=%v, #grants=%v", w.cl.Since(w.status.connected), len(w.grants))

	close(w.out)

	w.in = make(chan leader.Message)
	w.out = make(chan leader.Message)
	w.status = nil
	w.lease = nil

	for _, g := range w.grants {
		if g.State != LeaseRevoked {
			g.State = LeaseStale
		}
	}
}

func (w *Worker) process(ctx context.Context) {
	defer w.Close()
	defer w.resetMetrics(ctx)

	statsTimer := w.cl.NewTicker(statsDuration)
	defer statsTimer.Stop()

steady:
	for {
		select {
		case msg, ok := <-w.in:
			if !ok {
				log.Errorf(ctx, "Leader connection unexpectedly closed")
				w.lostLeader(ctx)
				break
			}
			w.handleMessage(ctx, msg)

		case <-w.expire:
			w.checkExpiration(ctx)

		case fn := <-w.inject:
			fn()

		case <-statsTimer.C:
			// record metrics
			log.Infof(ctx, "Worker %v, joined=%v, #services=%v, #grants=%v", w.self, w.status != nil, len(w.services), len(w.grants))
			w.emitMetrics(ctx)

		case <-w.drain.Closed():
			break steady

		case <-w.Closed():
			return
		}
	}

	log.Infof(ctx, "Worker %v draining, joined=%v, #grants=%v", w.self, w.status != nil, len(w.grants))
	now := w.cl.Now()

	if w.status == nil || !w.trySend(ctx, leader.NewDeregister()) {
		log.Errorf(ctx, "Worker %v draining while disconnected/stuck. Hard close", w.self)
		return
	}

	for {
		select {
		case msg, ok := <-w.in:
			if !ok {
				if len(w.grants) == 0 {
					log.Infof(ctx, "Worker %v drained after %v", w.self, w.cl.Since(now))
					w.lostLeader(ctx)
					return
				}
				log.Errorf(ctx, "Leader connection unexpectedly closed while draining. Hard close")
				w.lostLeader(ctx)
				return
			}
			w.handleMessage(ctx, msg)

		case <-w.expire:
			w.checkExpiration(ctx)

		case fn := <-w.inject:
			fn()

		case <-w.Closed():
			return
		}
	}
}

func (w *Worker) handleMessage(ctx context.Context, msg leader.Message) {
	switch {
	case msg.IsWorkerMessage():
		worker, _ := msg.WorkerMessage()
		w.handleWorkerMessage(ctx, worker)

	case msg.IsClusterMessage():
		cluster, _ := msg.ClusterMessage()
		w.handleClusterMessage(ctx, cluster)

	default:
		log.Errorf(ctx, "Internal: unexpected leader message: %v", msg)
	}
}

func (w *Worker) handleWorkerMessage(ctx context.Context, msg leader.WorkerMessage) {
	switch {
	case msg.IsAssign():
		assign, _ := msg.Assign()
		grant, state := assign.Grant(), assign.State()

		log.Infof(ctx, "Received assign %v", grant)

		w.services[grant.Service()] = grant.ID()
		if old, ok := w.grants[grant.ID()]; ok {
			if old.State == LeaseStale {
				old.State = LeaseActive
				old.Lease = w.lease

				log.Infof(ctx, "Re-activating stale grant %v", old)
				return
			} else {
				log.Errorf(ctx, "Internal: unexpected re-grant of non-stale grant %v. Re-creating", old)

				old.Coordinator.Close()
				delete(w.grants, grant.ID())
			}
		}

		updates := make(chan core.Update, coordinatorStateBufferLen)
		c := w.factory(ctx, grant.Service(), state, updates)

		w.grants[grant.ID()] = &Grant{
			Grant:       grant,
			State:       LeaseActive,
			Lease:       w.lease,
			Coordinator: c,
			Updates:     updates,
		}

		go func() {
			defer close(updates)

			<-c.Closed()
			syncx.Txn0(ctx, w.txn, func() {
				w.removeGrant(ctx, grant.ID())
			})
		}()

		log.Infof(ctx, "Created coordinator %v for grant %v", c, grant)

	case msg.IsRevoke():

		revoke, _ := msg.Revoke()
		grants := revoke.Grants()
		leases := map[time.Time]*clockx.Timer{}

		log.Infof(ctx, "Received revoke %v", grants)

		for _, g := range grants {
			gid := g.ID()
			grant, ok := w.grants[gid]
			if !ok {
				log.Warnf(ctx, "Revoking stale grant: %v. Ignoring", g)
				continue
			}

			// (1) Create lease that expires at the current TTL. The worker lease no longer includes this grant.
			// We share the leases to not get a flood of identical expiration checks.

			ttl := grant.Lease.Ttl()
			if _, ok := leases[ttl]; !ok {
				leases[ttl] = clockx.AfterFunc(w.cl, w.cl.Until(ttl), w.emitExpirationCheck)
			}
			grant.State = LeaseRevoked
			grant.Lease = leases[ttl]

			// (2) Revoke async. If the Revoke unexpectedly arrives before Assign, the lease still expires.

			log.Infof(ctx, "Revoking grant %v, ttl=%v", g, ttl)

			grant.Coordinator.Drain(w.cl.Until(ttl))
		}

	case msg.IsLeaseUpdate():
		lease, _ := msg.LeaseUpdate()

		if w.lease == nil {
			w.lease = clockx.AfterFunc(w.cl, w.cl.Until(lease.Ttl()), w.emitExpirationCheck)
		}
		w.lease.Reset(w.cl.Until(lease.Ttl()))

	case msg.IsUpdate():
		update, _ := msg.Update()

		grant, ok := w.grants[update.Grant().ID()]
		if !ok {
			log.Warnf(ctx, "Updating stale grant: %v. Ignoring", grant)
			return
		}

		select {
		case grant.Updates <- update.State():
		case <-time.After(100 * time.Millisecond):
			log.Errorf(ctx, "Internal: Coordinator %v unable to accept new state updates", grant.Coordinator)
			grant.Coordinator.Close()
		}

	default:
		log.Errorf(ctx, "Invalid worker message: %v", msg)
	}
}

func (w *Worker) handleClusterMessage(ctx context.Context, msg leader.ClusterMessage) {
	version := msg.Version()
	timestamp := msg.Timestamp()

	switch {
	case msg.IsSnapshot():
		snapshot, _ := msg.Snapshot()

		id := model.ClusterID{
			Version:   version,
			Timestamp: timestamp,
		}
		if instance, ok := snapshot.Origin(); ok {
			id.Origin = instance
		}

		w.cluster = core.NewCluster(id, snapshot.Assignments()...)

		log.Infof(ctx, "Received cluster snapshot %v", w.cluster.ID())

	case msg.IsUpdate(), msg.IsRemove():
		id := msg.ID()

		if w.cluster == nil {
			log.Errorf(ctx, "Internal: unexpected incremental update for uninitialized cluster: %v v%v. Disconnecting", id, version)
			w.lostLeader(ctx)
			return
		}
		if id != "" && !w.cluster.ID().IsNext(id, version) {
			log.Errorf(ctx, "Internal: unexpected incremental update for %v: %v v%v. Disconnecting", w.cluster.ID(), id, version)
			w.lostLeader(ctx)
			return
		}

		switch {
		case msg.IsUpdate():
			update, _ := msg.Update()
			w.cluster = core.UpdateCluster(w.cluster, update.Assignments(), nil, timestamp)

		case msg.IsRemove():
			remove, _ := msg.Remove()
			w.cluster = core.UpdateCluster(w.cluster, nil, remove.Services(), timestamp)
		}

	default:
		log.Errorf(ctx, "Invalid cluster message: %v", msg)
		return
	}

	chanx.Clear(w.clusters)
	w.clusters <- w.cluster
}

func (w *Worker) emitExpirationCheck() {
	select {
	case w.expire <- true:
	default:
		// skip: already flagged
	}
}

func (w *Worker) checkExpiration(ctx context.Context) {
	// Possible grant expiration

	now := w.cl.Now()
	for gid, g := range w.grants {
		if g.Lease.Ttl().Before(now) {
			w.removeGrant(ctx, gid)
		}
	}
}

func (w *Worker) removeGrant(ctx context.Context, gid core.GrantID) {
	grant, ok := w.grants[gid]
	if !ok {
		return // ok: no longer present
	}
	grant.Coordinator.Close()

	if grant.State != LeaseStale && w.cl.Now().Before(grant.Lease.Ttl()) {
		w.mustSend(ctx, leader.NewRelinquished(grant.Grant))
	}

	delete(w.grants, gid)
	if w.services[grant.Grant.Service()] == gid {
		delete(w.services, grant.Grant.Service()) // delete service tracking if not replaced by new grant
	}

	log.Infof(ctx, "Worker %v removed grant %v", w.self, grant)
}

func (w *Worker) emitMetrics(ctx context.Context) {
	w.resetMetrics(ctx)

	grants := map[model.QualifiedServiceName]map[LeaseState]int{}
	for _, grant := range w.grants {
		service := grant.Grant.Service()
		if grants[service] == nil {
			grants[service] = map[LeaseState]int{}
		}
		grants[service][grant.State] += 1
	}

	for service, counts := range grants {
		for state, count := range counts {
			numGrants.Set(ctx, float64(count), slicex.CopyAppend(core.QualifiedServiceTags(service), core.LeaseStateTag(string(state)))...)
		}
	}
}

func (w *Worker) resetMetrics(ctx context.Context) {
	numGrants.Reset(ctx)
}

func (w *Worker) trySend(ctx context.Context, msg leader.Message) bool {
	select {
	case w.out <- msg:
		return true
	default:
		return false
	}
}

func (w *Worker) mustSend(ctx context.Context, msg leader.Message) {
	if w.status == nil {
		return // ok: disconnected
	}

	timer := w.cl.NewTimer(100 * time.Millisecond)
	defer timer.Stop()

	select {
	case w.out <- msg:
		return
	case <-timer.C:
		log.Errorf(ctx, "Internal: leader connection stuck. Disconnecting")
		w.lostLeader(ctx)
	}
}

// Txn is a helper that constructs a syncx.TxnFn with the project specific error codes and injection channels
func (w *Worker) txn(ctx context.Context, fn func() error) error {
	var wg sync.WaitGroup
	var err error

	wg.Add(1)
	select {
	case w.inject <- func() {
		defer wg.Done()
		err = fn()
	}:
		wg.Wait()
		return err
	case <-ctx.Done():
		return model.ErrOverloaded
	case <-w.Closed():
		return model.ErrDraining
	}
}
