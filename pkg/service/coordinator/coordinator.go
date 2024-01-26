package coordinator

import (
	"context"
	"atoms.co/lib-go/pkg/clock"
	"go.atoms.co/splitter/lib/service/location"
	"go.atoms.co/splitter/lib/service/session"
	"go.atoms.co/lib/log"
	"go.atoms.co/lib/metrics"
	"go.atoms.co/lib/chanx"
	"go.atoms.co/lib/contextx"
	"go.atoms.co/lib/iox"
	"go.atoms.co/lib/mapx"
	"go.atoms.co/lib/randx"
	"go.atoms.co/slicex"
	"go.atoms.co/lib/syncx"
	"go.atoms.co/splitter/pkg/allocation"
	"go.atoms.co/splitter/pkg/core"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pkg/storage"
	"go.atoms.co/splitter/pkg/util/sessionx"
	"fmt"
	"sync"
	"time"
)

const (
	// leaseDuration is the duration of a consumer lease.
	leaseDuration = 40 * time.Second
)

var (
	numConsumers = metrics.NewTrackedGauge(
		metrics.NewGauge("go.atoms.co/splitter/coordinator_consumers", "Connected consumer status", slicex.CopyAppend(core.QualifiedServiceKeys, core.StatusKey)...),
	)
	numAssignments = metrics.NewTrackedGauge(
		metrics.NewGauge("go.atoms.co/splitter/coordinator_assignments", "Assignment count", slicex.CopyAppend(core.QualifiedDomainKeys, core.GrantStateKey)...),
	)

	numShards = metrics.NewTrackedGauge(
		metrics.NewGauge("go.atoms.co/splitter/coordinator_shards", "Shard count", core.QualifiedDomainKeys...),
	)

	numActions = metrics.NewCounter("go.atoms.co/splitter/coordinator_actions", "Leader actions", core.TenantKey, core.ServiceKey, core.ActionKey, core.ResultKey)
)

// Coordinator handles consumer connection and work allocation.
type Coordinator interface {
	iox.AsyncCloser

	Connect(ctx context.Context, sid session.ID, in <-chan model.ConsumerMessage) (<-chan model.ConsumerMessage, error)
	Drain(timeout time.Duration)
}

type Option func(*coordinator)

func WithFastActivation() Option {
	return func(c *coordinator) {
		c.fastActivation = true
	}
}

// coordinator is responsible for managing a single service. It accepts incoming connections from consumers and
// distributes work among the consumers by assigning shards with leases.
type coordinator struct {
	iox.AsyncCloser

	cl clock.Clock
	id location.Instance

	// options
	fastActivation bool

	name  model.QualifiedServiceName
	info  model.ServiceInfoEx
	cache *storage.Cache

	consumers map[model.InstanceID]*consumerSession
	alloc     *Allocation
	cluster   *model.ClusterMap
	messages  chan *sessionx.Message[model.ConsumerMessage]

	inject chan func()

	drain, initialized iox.AsyncCloser
}

func New(ctx context.Context, cl clock.Clock, loc location.Location, service model.QualifiedServiceName, state core.State, updates <-chan core.Update, opts ...Option) *coordinator {
	c := &coordinator{
		AsyncCloser: iox.WithQuit(ctx.Done(), iox.NewAsyncCloser()),
		cl:          cl,
		id:          location.NewNamedInstance("coordinator", loc),
		name:        service,
		cache:       storage.NewCache(),
		consumers:   map[model.InstanceID]*consumerSession{},
		messages:    make(chan *sessionx.Message[model.ConsumerMessage], 1000),
		inject:      make(chan func()),
		initialized: iox.NewAsyncCloser(),
		drain:       iox.NewAsyncCloser(),
	}
	for _, opt := range opts {
		opt(c)
	}

	go c.init(core.NewServiceContext(context.Background(), service), state, updates)

	return c
}

func (c *coordinator) Connect(ctx context.Context, sid session.ID, in <-chan model.ConsumerMessage) (<-chan model.ConsumerMessage, error) {
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

	return syncx.Txn1(ctx, c.txn, func() (<-chan model.ConsumerMessage, error) {
		wctx, cancel := contextx.WithQuitCancel(context.Background(), c.Closed())

		s, out := c.connect(wctx, sid, register, in)
		c.allocate(ctx, c.cl.Now(), false) // Allocate to assign work post connection

		go func() {
			defer cancel()
			<-s.connection.Closed()

			syncx.AsyncTxn(c.txn, func() {
				if cur, ok := c.consumers[s.consumer.ID()]; ok {
					if ok && cur.connection.Sid() == sid { // guard against race
						c.disconnect(wctx, "connection closed", cur)
					}
				}
			})
		}()
		return out, nil
	})
}

func (c *coordinator) Initialized() iox.AsyncCloser {
	return c.initialized
}

func (c *coordinator) connect(ctx context.Context, sid session.ID, register model.RegisterMessage, in <-chan model.ConsumerMessage) (*consumerSession, <-chan model.ConsumerMessage) {
	consumer := NewConsumer(register.Consumer(), c.cl.Now())

	var active []Grant
	for _, g := range register.Active() {
		grant, err := fromGrant(consumer)(g)
		if err != nil {
			log.Errorf(ctx, "Internal: invalid grant %v: %v", g, err)
			continue
		}
		active = append(active, grant)
	}

	if old, ok := c.consumers[consumer.ID()]; ok {
		log.Infof(ctx, "Consumer %v re-connected (session=%v) with #grants: %d. Disconnecting stale session", consumer, sid, len(active))
		c.disconnect(ctx, "reconnect", old)
	} else {
		log.Infof(ctx, "Consumer %v connected (session=%v) with #grants: %d", consumer, sid, len(active))
	}

	connection, out := sessionx.NewConnection[model.ConsumerMessage](c.cl, sid, consumer.Instance(), c, in, c.messages)
	s := &consumerSession{
		consumer:   consumer,
		connection: connection,
	}
	c.consumers[consumer.ID()] = s

	lease := c.cl.Now().Add(leaseDuration)
	s.TrySend(ctx, model.NewExtend(lease)) // grants will be covered under this lease

	if assigned, ok := c.alloc.Attach(allocation.NewWorker(consumer.instance.Instance().ID(), consumer.instance), lease, active...); ok {
		log.Infof(ctx, "attached returned: %v", assigned)
		if len(assigned.Active) > 0 {
			s.TrySend(ctx, model.NewAssign(slicex.Map(assigned.Active, toGrant)...))
		}
		if len(assigned.Allocated) > 0 {
			s.TrySend(ctx, model.NewAssign(slicex.Map(assigned.Allocated, toGrant)...))
		}
	}

	// Send full cluster to the consumer
	if !s.TrySend(ctx, model.NewClusterSnapshot(c.cluster.ID(), model.ClusterToAssignments(c.cluster)...)) {
		log.Errorf(ctx, "Internal: failed to send initial cluster map to consumer: %v. Closing", s)
		connection.Disconnect()
	}

	return s, out
}

func (c *coordinator) disconnect(ctx context.Context, reason string, consumers ...*consumerSession) {
	for _, s := range consumers {
		log.Infof(ctx, "Disconnecting consumer (reason: %v): %v", reason, s)
		recordAction(ctx, "disconnect", "ok")

		if c.alloc.Detach(s.consumer.ID()) {
			assigned := c.alloc.Assigned(s.ID())
			if len(assigned.Active) > 0 || len(assigned.Allocated) > 0 {
				log.Warnf(ctx, "Detached consumer %v with %v active and %v allocated domains", s.consumer, len(assigned.Active), len(assigned.Allocated))
			}
		} // else: already detached

		s.connection.Disconnect()
		delete(c.consumers, s.consumer.ID())
	}
}

func (c *coordinator) init(ctx context.Context, state core.State, updates <-chan core.Update) {
	defer c.Close()
	defer c.drain.Close()
	defer c.initialized.Close()

	c.cache.Restore(core.NewSnapshot(state))

	info, ok := c.cache.Service(c.name)
	if !ok {
		log.Errorf(ctx, "Internal: invalid state for coordinator %v/%v", c.name, c.id)
		return
	}
	c.info = info

	delay := leaseDuration
	if c.fastActivation {
		delay = 0
	}
	c.alloc = newAllocation(c.id.ID(), info, c.cache.Placements(c.name.Tenant), c.cl.Now().Add(delay))
	c.cluster, _ = toCluster(c.alloc, model.ClusterID{Origin: c.id, Version: 1, Timestamp: c.cl.Now()})

	log.Infof(ctx, "Coordinator %v/%v initialized, #shards=%v", c.name, c.id, c.alloc.Size())
	recordAction(ctx, "init", "ok")
	c.initialized.Close()

	c.process(ctx, updates)

	// Close consumer connections
	for _, s := range c.consumers {
		s.connection.Disconnect()
	}

	log.Infof(ctx, "Coordinator %v/%v closed", c.name, c.id)
}

func (c *coordinator) process(ctx context.Context, updates <-chan core.Update) {
	defer c.resetMetrics(ctx)

	ticker := c.cl.NewTicker(10*time.Second + randx.Duration(time.Second))
	defer ticker.Stop()

	cluster := c.cl.NewTicker(100*time.Millisecond + randx.Duration(50*time.Millisecond))
	defer cluster.Stop()

	var broadcast bool

steady:
	for {
		select {
		case msg := <-c.messages:
			s, ok := c.consumers[msg.Instance.ID()]
			if !ok || msg.Sid != s.connection.Sid() {
				log.Debugf(ctx, "Ignoring stale message from consumer session %v: %v", msg.Sid, msg)
				break
			}
			c.handleConsumerMessage(ctx, s, msg.Msg)
			broadcast = true // possible change

		case upd := <-updates:
			// (1) Refresh allocation, (2) allocate, (3) broadcast cluster change

			if err := c.cache.Update(upd, false); err != nil {
				log.Errorf(ctx, "Internal: invalid state update %v", err)
				return
			}
			info, ok := c.cache.Service(c.name)
			if !ok {
				log.Errorf(ctx, "Internal: coordinator service not present in updated state. Closing")
				return
			}
			c.info = info

			c.refresh(ctx, leaseDuration)
			c.allocate(ctx, c.cl.Now(), false)
			c.broadcast(ctx)

		case <-ticker.C:
			// (1) Send lease updates, (2) disconnect unhealthy consumers, (3) allocate
			// (4) broadcast cluster changes (5) emit metrics

			now := c.cl.Now()
			lease := now.Add(leaseDuration)

			var unhealthy []*consumerSession
			for _, s := range c.consumers {
				if !s.connection.Send(ctx, model.NewExtend(lease)) {
					unhealthy = append(unhealthy, s)
					continue
				}
				c.alloc.Extend(s.consumer.ID(), lease)
			}
			c.disconnect(ctx, "unhealthy", unhealthy...)
			c.allocate(ctx, now, true)
			c.broadcast(ctx)
			c.emitMetrics(ctx)

		case <-cluster.C:
			// (1) Broadcast cluster changes
			if broadcast {
				c.broadcast(ctx)
				broadcast = false // wait for possible change
			}

		case fn := <-c.inject:
			fn()

		case <-c.drain.Closed():
			break steady

		case <-c.Closed():
			return
		}
	}

	log.Infof(ctx, "Coordinator %v draining, #consumer=%v", c.id, len(c.consumers))
}

func (c *coordinator) refresh(ctx context.Context, delay time.Duration) {
	now := c.cl.Now()

	upd, rejected := updateAllocation(c.alloc, c.info, c.cache.Placements(c.name.Tenant), now.Add(delay))
	c.alloc = upd

	for _, g := range rejected {
		c.mustSend(ctx, c.consumers[g.Worker], model.NewRevoke(toGrant(g)))
	}
}

func (c *coordinator) allocate(ctx context.Context, now time.Time, loadbalance bool) {
	// (1) Expire, Allocate and LoadBalance. If any worker cannot handle the update
	// they are disconnected. If an assignment fails, the grant is immediately released.

	promoted := c.alloc.Expire(now)
	grants := c.alloc.Allocate(now)

	if loadbalance {
		// Revoke and allocate

		if move, load, ok := c.alloc.LoadBalance(now); ok {
			// Revoke from source worker, on failure, lease will run out
			s := c.consumers[move.From.Worker]
			if !s.TrySend(ctx, model.NewRevoke(toGrant(move.From))) {
				log.Errorf(ctx, "Failed to send revoke for move %v to consumer: %v. Disconnecting", move, s)
				c.disconnect(ctx, "stuck", s)
			}

			// Allocate to destination worker, on failure, release allocation
			s = c.consumers[move.To.Worker]
			if !s.TrySend(ctx, model.NewAssign(toGrant(move.To))) {
				log.Errorf(ctx, "Failed to send allocate for move %v to consumer: %v. Disconnecting", move, s)
				c.alloc.Release(move.To, now)
				c.disconnect(ctx, "stuck", s)
			}

			log.Debugf(ctx, "Initiated grant move: %v, load=%v", move, load)
			recordAction(ctx, "move", "ok")
		}
	}

	c.assign(ctx, now, grants...)
	c.promote(ctx, promoted...)

	log.Debugf(ctx, "Allocation: %v", c.alloc)
}

func (c *coordinator) assign(ctx context.Context, now time.Time, grants ...Grant) {
	if len(grants) == 0 {
		return
	}

	// (1) Assign all grants, if possible. Release un-assignable grants.

	for _, grant := range grants {
		s, ok := c.consumers[grant.Worker]
		if !ok {
			c.alloc.Release(grant, now) // undo assignment
			continue
		}

		if !s.TrySend(ctx, model.NewAssign(toGrant(grant))) {
			log.Errorf(ctx, "Failed to send assignment for grant %v to consumer: %v. Disconnecting", grant, s)

			c.alloc.Release(grant, now) // undo assignment. Safe because it was not sent
			c.disconnect(ctx, "stuck", s)
			recordAction(ctx, "assign", "failed")
			continue
		}
		recordAction(ctx, "assign", "ok")

		log.Infof(ctx, "Assigned new grant to consumer %v: %v.", s, grant)
	}
}

func (c *coordinator) promote(ctx context.Context, grants ...Grant) {
	if len(grants) == 0 {
		return
	}

	// (1) Promote all grants, if possible, otherwise leave grant as is.

	for _, grant := range grants {
		s, ok := c.consumers[grant.Worker]
		if !ok {
			continue
		}

		if !s.TrySend(ctx, model.NewPromote(toGrant(grant))) {
			log.Errorf(ctx, "Failed to send promotion for grant %v to consumer: %v. Disconnecting", grant, s)
			c.disconnect(ctx, "stuck", s)
			recordAction(ctx, "promote", "failed")
			continue
		}
		recordAction(ctx, "promote", "ok")

		log.Infof(ctx, "Allocated new grant to consumer %v: %v.", s, grant)
	}
}

func (c *coordinator) handleConsumerMessage(ctx context.Context, s *consumerSession, m model.ConsumerMessage) {
	msg, ok := m.ClientMessage()
	if !ok {
		log.Errorf(ctx, "Internal: unexpected message: %v", m)
		c.disconnect(ctx, "invalid message", s)
		return
	}

	switch {
	case msg.IsDeregister():
		deregister, _ := msg.Deregister()
		c.handleDeregister(ctx, s, deregister)

	case msg.IsReleased():
		released, _ := msg.Released()
		c.handleReleased(ctx, s, released)

	default:
		log.Errorf(ctx, "Internal: unexpected consumer message: %v", msg)
	}
}

func (c *coordinator) handleDeregister(ctx context.Context, s *consumerSession, deregister model.DeregisterMessage) {
	log.Infof(ctx, "Received de-register from consumer %v", s)

	// (1) Mark worker as suspended to prevent new work.

	s.draining = true
	c.alloc.Suspend(s.consumer.ID())

	// (2) Revoke all active grants. The leader sends out new assignments only on Active grants,
	// so Allocated grants can just be released. Revoked assignments are already in progress.

	now := c.cl.Now()

	assigned := c.alloc.Assigned(s.consumer.ID())
	for _, g := range assigned.Allocated {
		c.alloc.Release(g, now) // no promotion
	}
	if len(assigned.Active) > 0 {
		c.alloc.Revoke(s.consumer.ID(), now, assigned.Active...)

		if !s.TrySend(ctx, model.NewRevoke(slicex.Map(assigned.Active, toGrant)...)) {
			log.Errorf(ctx, "Failed to revoke %v grants for worker: %v. Disconnecting", len(assigned.Active), s)
			c.disconnect(ctx, "stuck", s)
			return
		}
	}

	// (3) If no grants, disconnect immediately. Otherwise, wait for last release or expiration.

	if assigned.IsEmpty() {
		c.disconnect(ctx, "no grant deregister", s) // no grants, safe to disconnect
		return
	}

	log.Infof(ctx, "Deregistered consumer %v with %v active grants", s, len(assigned.Active))
}

func (c *coordinator) handleReleased(ctx context.Context, s *consumerSession, released model.ReleasedMessage) {
	log.Infof(ctx, "Received released %v from consumer %v", released, s)

	now := c.cl.Now()

	// (1) Release relinquished grants. Check deregister status

	var promoted []Grant
	for _, g := range released.Grants() {
		grant, err := fromGrant(s.consumer)(g)
		if err != nil {
			log.Errorf(ctx, "Internal: invalid grant %v: %v", g, err)
			continue
		}
		if promo, ok := c.alloc.Release(grant, now); ok {
			promoted = append(promoted, promo)
		}
	}

	if s.draining && c.alloc.Assigned(s.consumer.ID()).IsEmpty() {
		c.disconnect(ctx, "complete deregister", s)
		c.alloc.Remove(s.consumer.ID())
	}

	// (2) Promote grants, if any

	c.promote(ctx, promoted...)
}

func (c *coordinator) broadcast(ctx context.Context) {
	newCluster, err := toCluster(c.alloc, model.ClusterID{Origin: c.id, Version: c.cluster.ID().Version + 1, Timestamp: c.cl.Now()})
	if err != nil {
		log.Errorf(ctx, "Internal: unable to create cluster from allocation: %v", err)
		return
	}

	d := newCluster.Diff(c.cluster)
	if d.IsEmpty() {
		return
	}

	assigned := mapx.MapToSlice(d.Assigned, func(cid model.ConsumerID, grants []model.GrantInfo) model.Assignment {
		consumer, _, _ := newCluster.Consumer(cid)
		return model.NewAssignment(consumer, grants...)
	})

	c.cluster = newCluster

	for _, s := range c.consumers {
		c.mustSend(ctx, s, model.NewClusterChange(newCluster.ID(), assigned, d.Updated, d.Unassigned, d.Removed))
	}

	log.Debugf(ctx, "Sent cluster update: %v/%v. Cluster: %v", c.name, c.alloc, newCluster)
}

// mustSend attempts to send a message on a consumer connection. If it fails, disconnect the consumer
func (c *coordinator) mustSend(ctx context.Context, s *consumerSession, message model.ConsumerMessage) bool {
	if s == nil {
		return false // already disconnected
	}
	if !s.connection.Send(ctx, message) {
		c.disconnect(ctx, "stuck", s)
		return false
	}
	return true
}

func (c *coordinator) Drain(timeout time.Duration) {
	c.drain.Close()
	c.cl.AfterFunc(timeout, c.Close)
}

func (c *coordinator) String() string {
	return fmt.Sprintf("%v[alloc=%v, #consumers=%v]", c.name, c.alloc, len(c.consumers))
}

func (c *coordinator) emitMetrics(ctx context.Context) {
	c.resetMetrics(ctx)

	// Consumers with status
	numConsumers.Set(ctx, float64(len(c.consumers)), slicex.CopyAppend(core.QualifiedServiceTags(c.name), core.StatusTag("ok"))...)

	// Static shard counts
	for _, domain := range c.cache.Domains(c.name) {
		shards := domain.Config().ShardingPolicy().Shards()
		numShards.Set(ctx, float64(shards), core.QualifiedDomainTags(domain.Name())...)
	}

	// Assignments with state
	assigned := map[model.QualifiedDomainName]map[model.GrantState]int{}
	for _, worker := range c.alloc.Workers() {
		assign := c.alloc.Assigned(worker.ID())
		for _, active := range assign.Active {
			if assigned[active.Unit.Domain] == nil {
				assigned[active.Unit.Domain] = map[model.GrantState]int{}
			}
			assigned[active.Unit.Domain][model.ActiveGrantState] += 1
		}
		for _, allocated := range assign.Allocated {
			if assigned[allocated.Unit.Domain] == nil {
				assigned[allocated.Unit.Domain] = map[model.GrantState]int{}
			}
			assigned[allocated.Unit.Domain][model.AllocatedGrantState] += 1
		}
		for _, revoked := range assign.Revoked {
			if assigned[revoked.Unit.Domain] == nil {
				assigned[revoked.Unit.Domain] = map[model.GrantState]int{}
			}
			assigned[revoked.Unit.Domain][model.RevokedGrantState] += 1
		}
	}
	for domain, counts := range assigned {
		for state, count := range counts {
			numAssignments.Set(ctx, float64(count), slicex.CopyAppend(core.QualifiedDomainTags(domain), core.GrantStateTag(state))...)
		}
	}
}

func (c *coordinator) resetMetrics(ctx context.Context) {
	numConsumers.Reset(ctx, core.QualifiedServiceTags(c.name)...)
	numShards.Reset(ctx, core.QualifiedServiceTags(c.name)...)
	numAssignments.Reset(ctx, core.QualifiedServiceTags(c.name)...)
}

// Txn is a helper that constructs a syncx.TxnFn with the project specific error codes and injection channels
func (c *coordinator) txn(ctx context.Context, fn func() error) error {
	var wg sync.WaitGroup
	var err error

	wg.Add(1)
	select {
	case c.inject <- func() {
		defer wg.Done()
		err = fn()
	}:
		wg.Wait()
		return err
	case <-ctx.Done():
		return model.ErrOverloaded
	case <-c.Closed():
		return model.ErrDraining
	}
}

func recordAction(ctx context.Context, action, result string) {
	numActions.Increment(ctx, 1, core.ActionTag(action), core.ResultTag(result))
}
