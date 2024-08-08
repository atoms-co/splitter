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
	"go.atoms.co/splitter/pb/private"
	"go.atoms.co/splitter/pb"
	"fmt"
	"maps"
	"sync"
	"time"
)

const (
	// handleTimeout is the timeout for handle requests.
	handleTimeout = 5 * time.Second
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
	numAssignmentsByLocation = metrics.NewTrackedGauge(
		metrics.NewGauge("go.atoms.co/splitter/coordinator_assignments_by_location", "Assignment by location", slicex.CopyAppend(core.QualifiedServiceKeys, core.InstanceIDKey, core.LocationKey)...),
	)
	numLoadByLocation = metrics.NewTrackedGauge(
		metrics.NewGauge("go.atoms.co/splitter/coordinator_load_by_location", "Load by location", slicex.CopyAppend(core.QualifiedServiceKeys, core.InstanceIDKey, core.LocationKey)...),
	)
	numPlacementByLocation = metrics.NewTrackedGauge(
		metrics.NewGauge("go.atoms.co/splitter/coordinator_placement_by_location", "Placement by location", slicex.CopyAppend(core.QualifiedServiceKeys, core.InstanceIDKey, core.LocationKey)...),
	)
	numColocationByLocation = metrics.NewTrackedGauge(
		metrics.NewGauge("go.atoms.co/splitter/coordinator_colocation_by_location", "Colocation by location", slicex.CopyAppend(core.QualifiedServiceKeys, core.InstanceIDKey, core.LocationKey)...),
	)
	numActions       = metrics.NewCounter("go.atoms.co/splitter/coordinator_actions", "Coordinator actions", core.TenantKey, core.ServiceKey, core.ActionKey, core.ResultKey)
	numActionLatency = metrics.NewHistogram("go.atoms.co/splitter/coordinator_action_latency", "Coordinator action latency", nil, core.TenantKey, core.ServiceKey, core.ActionKey)
)

// Coordinator handles consumer connection and work allocation.
type Coordinator interface {
	iox.AsyncCloser

	Initialized() iox.RAsyncCloser
	Connect(ctx context.Context, sid session.ID, in <-chan model.ConsumerMessage) (<-chan model.ConsumerMessage, error)
	Handle(ctx context.Context, request HandleRequest) (*internal_v1.CoordinatorHandleResponse, error)
	Drain(timeout time.Duration)
}

type Option func(*coordinator)

func WithFastActivation() Option {
	return func(c *coordinator) {
		c.fastActivation = true
	}
}

func WithRefreshDelay(delay time.Duration) Option {
	return func(c *coordinator) {
		c.refreshDelay = delay
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
	refreshDelay   time.Duration

	name   model.QualifiedServiceName
	tenant model.TenantInfo
	info   model.ServiceInfoEx
	cache  *storage.Cache

	consumers map[model.InstanceID]*consumerSession
	alloc     *Allocation
	cluster   *model.ClusterMap
	messages  chan *sessionx.Message[model.ConsumerMessage]

	inject chan func()

	drain, initialized iox.AsyncCloser
}

func New(ctx context.Context, cl clock.Clock, loc location.Location, service model.QualifiedServiceName, state core.State, updates <-chan core.Update, opts ...Option) Coordinator {
	c := &coordinator{
		AsyncCloser:  iox.WithQuit(ctx.Done(), iox.NewAsyncCloser()),
		cl:           cl,
		id:           location.NewNamedInstance("coordinator", loc),
		refreshDelay: leaseDuration,
		name:         service,
		cache:        storage.NewCache(),
		consumers:    map[model.InstanceID]*consumerSession{},
		messages:     make(chan *sessionx.Message[model.ConsumerMessage], 1000),
		inject:       make(chan func()),
		initialized:  iox.NewAsyncCloser(),
		drain:        iox.NewAsyncCloser(),
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

		var keys []model.QualifiedDomainKey
		var err error

		opts := register.Options()
		if len(opts.DomainKeyNames()) > 0 {
			keys, err = c.findNamedKeys(opts.DomainKeyNames())
			if err != nil {
				return nil, model.WrapError(fmt.Errorf("invalid canary named keys, %v: %w", err, model.ErrInvalid))
			}
		}

		s, out := c.connect(wctx, sid, register, opts.CapacityLimit(), keys, in)

		// Refresh allocation rules on consumer connect if using named keys
		if len(s.consumer.Keys()) > 0 {
			c.refresh(ctx, 0)
		}
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

func (c *coordinator) Handle(ctx context.Context, req HandleRequest) (*internal_v1.CoordinatorHandleResponse, error) {
	wctx, cancel := context.WithTimeout(ctx, handleTimeout)
	defer cancel()

	resp, err := c.handle(wctx, req)
	if err != nil {
		log.Infof(ctx, "Coordinator %v request %v failed: %v", c.id, req, err)
		return nil, model.WrapError(err)
	}
	return resp, nil
}

func (c *coordinator) handle(ctx context.Context, req HandleRequest) (*internal_v1.CoordinatorHandleResponse, error) {
	switch {
	case req.Proto.GetOperation() != nil:
		ret, err := c.handleOperationRequest(ctx, req.Proto.GetOperation())
		if err != nil {
			return nil, err
		}
		return NewHandleCoordinatorOperationResponse(ret), nil

	default:
		return nil, fmt.Errorf("invalid handle request: %v", req)
	}
}

func (c *coordinator) handleOperationRequest(ctx context.Context, op *internal_v1.CoordinatorOperationRequest) (*internal_v1.CoordinatorOperationResponse, error) {
	switch {
	case op.GetInfo() != nil:
		return c.handleServiceInfoRequest(ctx)

	case op.GetRestart() != nil:
		return c.handleServiceRestartRequest(ctx)

	case op.GetSuspend() != nil:
		return c.handleConsumerSuspendRequest(ctx, model.InstanceID(op.GetSuspend().GetConsumerId()))

	case op.GetResume() != nil:
		return c.handleConsumerResumeRequest(ctx, model.InstanceID(op.GetResume().GetConsumerId()))

	case op.GetDrain() != nil:
		return c.handleConsumerDrainRequest(ctx, model.InstanceID(op.GetDrain().GetConsumerId()))

	default:
		return nil, fmt.Errorf("invalid operation request: %v", op)
	}
}

func (c *coordinator) Initialized() iox.RAsyncCloser {
	return c.initialized
}

func (c *coordinator) connect(ctx context.Context, sid session.ID, register model.RegisterMessage, limit int, keys []model.QualifiedDomainKey, in <-chan model.ConsumerMessage) (*consumerSession, <-chan model.ConsumerMessage) {
	now := c.cl.Now()

	consumer := NewConsumer(register.Consumer(), now, WithKeys(keys...))

	// Parse returning grants, active grants will be retained by the consumer
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

	lease := now.Add(leaseDuration)
	s.TrySend(ctx, model.NewExtend(lease)) // grants will be covered under this lease

	capacity := limit * int(ShardLoad) // Set capacity shard limit * shard load (0 for no capacity)
	if capacity > 0 {
		log.Infof(ctx, "Consumer %v connected with non-zero capacity limit: %v", consumer, capacity)
	}

	if assigned, ok := c.alloc.Attach(allocation.NewWorker(consumer.instance.ID(), consumer), allocation.Load(capacity), lease, active...); ok {
		if len(assigned.Active) > 0 {
			s.TrySend(ctx, model.NewAssign(slicex.Map(assigned.Active, toGrant)...))
		}
		if len(assigned.Allocated) > 0 {
			s.TrySend(ctx, model.NewAssign(slicex.Map(assigned.Allocated, toGrant)...))
		}
	}

	// Send full cluster to the consumer
	if !s.TrySend(ctx, model.NewClusterMessage(model.NewClusterSnapshot(c.cluster.ID(), c.cluster.Assignments(), c.alloc.Units()))) {
		log.Errorf(ctx, "Internal: failed to send initial cluster map to consumer: %v. Closing", s)
		connection.Disconnect()
	}

	return s, out
}

func (c *coordinator) findNamedKeys(named []model.DomainKeyName) ([]model.QualifiedDomainKey, error) {
	var ret []model.QualifiedDomainKey

	domains := mapx.New(c.info.Domains(), func(d model.Domain) model.DomainName {
		return d.ShortName()
	})

	for _, name := range named {
		d, ok := domains[name.Domain]
		if !ok {
			return nil, fmt.Errorf("unknown domain: %v", name.Domain)
		}
		first, ok := slicex.First(d.Config().NamedDomainKeys(), func(key model.NamedDomainKey) bool {
			return key.Name == name.Name
		})
		if !ok {
			return nil, fmt.Errorf("unknown named key: %v", name)
		}
		ret = append(ret, model.QualifiedDomainKey{Domain: d.Name(), Key: first.Key})
	}

	return ret, nil
}

func (c *coordinator) disconnect(ctx context.Context, reason string, consumers ...*consumerSession) {
	for _, s := range consumers {
		log.Infof(ctx, "Disconnecting consumer (reason: %v): %v", reason, s)
		c.recordAction(ctx, "disconnect", "ok")

		if len(s.consumer.Keys()) > 0 {
			// Refresh allocation rules on disconnect if using named keys
			c.refresh(ctx, 0)
		}

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

	start := c.cl.Now()

	c.cache.Restore(core.NewSnapshot(state))

	tenant, ok := c.cache.Tenant(c.name.Tenant)
	if !ok {
		log.Errorf(ctx, "Internal: invalid state for coordinator, tenant not found %v/%v", c.name, c.id)
		return
	}
	c.tenant = tenant

	info, ok := c.cache.Service(c.name)
	if !ok {
		log.Errorf(ctx, "Internal: invalid state for coordinator, service not found %v/%v", c.name, c.id)
		return
	}
	c.info = info

	delay := leaseDuration
	if c.fastActivation {
		delay = 0
	}

	now := c.cl.Now()
	c.alloc = newAllocation(c.id.ID(), tenant, info, c.cache.Placements(c.name.Tenant), now.Add(delay))
	c.cluster = model.NewClusterMap(model.NewClusterID(c.id, now), c.alloc.Units())

	log.Infof(ctx, "Coordinator %v/%v initialized, #shards=%v", c.name, c.id, c.alloc.Size())
	c.recordAction(ctx, "init", "ok")
	c.recordActionLatency(ctx, "init", start)
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

			now := c.cl.Now()

			s, ok := c.consumers[msg.Instance.ID()]
			if !ok || msg.Sid != s.connection.Sid() {
				log.Infof(ctx, "Ignoring stale message from consumer session %v: %v", msg.Sid, msg)
				break
			}
			c.handleConsumerMessage(ctx, s, msg.Msg)

			broadcast = true // possible change

			c.recordActionLatency(ctx, "consumer_message", now)

		case upd := <-updates:
			// (1) Refresh allocation, (2) allocate, (3) broadcast cluster change

			now := c.cl.Now()

			if err := c.cache.Update(upd, false); err != nil {
				log.Errorf(ctx, "Internal: invalid state update %v", err)
				return
			}

			tenant, ok := c.cache.Tenant(c.name.Tenant)
			if !ok {
				log.Errorf(ctx, "Internal: coordinator tenant not present in updated state. Closing")
				return
			}
			c.tenant = tenant

			info, ok := c.cache.Service(c.name)
			if !ok {
				log.Errorf(ctx, "Internal: coordinator service not present in updated state. Closing")
				return
			}
			c.info = info

			oldShards := c.alloc.Units()

			c.refresh(ctx, c.refreshDelay)
			c.allocate(ctx, c.cl.Now(), false)

			newShards := c.alloc.Units()
			var bopts []broadcastOption
			if !maps.Equal(slicex.NewSet(oldShards...), slicex.NewSet(newShards...)) {
				// Send list of shards if shards have changed.
				bopts = append(bopts, withSendingShards(newShards))
				c.broadcast(ctx, bopts...)
			}

			c.recordActionLatency(ctx, "tenant_update", now)

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
			c.emitMetrics(ctx)

			c.recordActionLatency(ctx, "tick", now)

		case <-cluster.C:
			// (1) Broadcast cluster changes

			now := c.cl.Now()

			if broadcast {
				c.broadcast(ctx)
				broadcast = false // wait for possible change
			}

			c.recordActionLatency(ctx, "tick/cluster", now)

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

	// Check if using named keys
	var isKeys bool
	var keys []model.QualifiedDomainKey
	for _, worker := range c.alloc.Workers() {
		if len(worker.Instance.Data.Keys()) > 0 {
			isKeys = true
			keys = append(keys, worker.Instance.Data.Keys()...)
		}
	}

	// Compute named shards
	var namedShards []model.Shard
	if isKeys {
		for _, work := range c.alloc.Work() {
			for _, key := range keys {
				if work.Unit.Contains(key) {
					namedShards = append(namedShards, work.Unit)
					break
				}
			}
		}
	}

	upd, rejected := updateAllocation(c.alloc, c.tenant, c.info, namedShards, c.cache.Placements(c.name.Tenant), now.Add(delay))
	c.alloc = upd

	for _, g := range rejected {
		c.mustSend(ctx, c.consumers[g.Worker], model.NewRevoke(toGrant(g)))
	}
}

func (c *coordinator) allocate(ctx context.Context, now time.Time, loadbalance bool) {
	defer c.recordActionLatency(ctx, "allocate", c.cl.Now()) // uses actual current time (vs _now_ param) for latency recording

	// (1) Expire, Allocate and LoadBalance. If any worker cannot handle the update
	// they are disconnected. If an assignment fails, the grant is immediately released.

	promoted := c.alloc.Expire(now)
	grants := c.alloc.Allocate(now)

	// Assign and allocate grants before load balancing
	// TODO(jhhurwitz): 06/11/2024 We should consider not assigning grants we know will be moved during load-balancing. Rather assign to the final destination only.
	c.assign(ctx, now, grants...)
	c.promote(ctx, promoted...)

	if loadbalance {
		// Revoke and allocate

		if move, load, ok := c.loadBalance(ctx, now); ok {
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

			log.Infof(ctx, "Initiated grant move: %v, load=%v", move, load)
			c.recordAction(ctx, "move", "ok")
		}
	}

	// broadcast cluster changes
	c.broadcast(ctx)

	log.Infof(ctx, "Allocation %v: %v", c.name, c.alloc)
}

func (c *coordinator) loadBalance(ctx context.Context, now time.Time) (allocation.Move[model.Shard, model.ConsumerID], allocation.AdjustedLoad, bool) {
	defer c.recordActionLatency(ctx, "loadbalance", c.cl.Now()) // uses actual current time (vs _now_ param) for latency recording
	return c.alloc.LoadBalance(now)
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
			c.recordAction(ctx, "assign", "failed")
			continue
		}
		c.recordAction(ctx, "assign", "ok")

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
			c.recordAction(ctx, "promote", "failed")
			continue
		}
		c.recordAction(ctx, "promote", "ok")

		log.Infof(ctx, "Promoted new grant for consumer %v: %v.", s, grant)
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

	case msg.IsUpdate():
		update, _ := msg.Update()
		c.handleUpdate(ctx, s, update)

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

	// (2) Revoke all active grants. The coordinator sends out new assignments only on Active grants,
	// so Allocated grants can just be released. Revoked assignments are already in progress.

	now := c.cl.Now()

	assigned := c.alloc.Assigned(s.consumer.ID())
	for _, g := range assigned.Allocated {
		c.alloc.Release(g, now) // no promotion
	}
	if len(assigned.Active) > 0 {
		revoked, _ := c.alloc.Revoke(s.consumer.ID(), now, assigned.Active...)
		if !s.TrySend(ctx, model.NewRevoke(slicex.Map(revoked, toGrant)...)) {
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

	// (4) Allocate unassigned grants
	c.allocate(ctx, c.cl.Now(), false)

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

func (c *coordinator) handleUpdate(ctx context.Context, s *consumerSession, update model.UpdateMessage) {
	log.Infof(ctx, "Received update %v from consumer %v", update, s)

	// (1) Update internal grant modifier and notify affected grant targets

	g, err := fromGrant(s.consumer)(update.Grant())
	if err != nil {
		log.Errorf(ctx, "Internal: invalid grant %v: %v", g, err)
		return
	}

	if !c.alloc.Modify(s.consumer.ID(), g) {
		log.Warnf(ctx, "Unable to modify grant %v", g)
		return
	}

	id, transition, ok := c.alloc.Transition(g)
	if !ok {
		log.Warnf(ctx, "Unable to find buddy for grant %v", g)
		return
	}

	t, ok := c.consumers[id]
	if !ok {
		log.Warnf(ctx, "Buddy grant owner %v not currently connected for transition grant %v", id, transition)
		return
	}

	if !t.TrySend(ctx, model.NewNotify(update.Grant(), toGrant(transition))) {
		log.Errorf(ctx, "Failed to send notify for grant %v for target %v on session %v. Disconnecting", g, transition, t)
		c.disconnect(ctx, "stuck", s)
		c.recordAction(ctx, "notify", "failed")
	}
	c.recordAction(ctx, "notify", "ok")
}

type broadcastOpts struct {
	sendShards bool
	shards     []model.Shard
}

type broadcastOption func(opts *broadcastOpts)

func withSendingShards(shards []model.Shard) broadcastOption {
	return func(opt *broadcastOpts) {
		opt.sendShards = true
		opt.shards = shards
	}
}

func (c *coordinator) broadcast(ctx context.Context, bopts ...broadcastOption) {
	defer c.recordActionLatency(ctx, "broadcast", c.cl.Now())

	var opts broadcastOpts
	for _, opt := range bopts {
		opt(&opts)
	}

	// (1) Compute difference, if any

	var assigned []model.Assignment
	var updated []model.GrantInfo
	var unassigned []model.GrantID
	var removed []model.ConsumerID

	for _, info := range c.alloc.Workers() {
		consumer := info.Instance.Data
		grants := c.alloc.Assigned(info.ID())

		if _, old, ok := c.cluster.Consumer(info.ID()); ok {
			present := map[model.GrantID]bool{}

			var fresh []model.GrantInfo

			for _, list := range [][]Grant{grants.Allocated, grants.Active, grants.Revoked} {
				for _, g := range list {
					ginfo := toGrantInfo(g)
					if _, prior, ok := c.cluster.Grant(g.ID); ok && !ginfo.Equals(prior) {
						updated = append(updated, ginfo)
					} else if !ok {
						fresh = append(fresh, ginfo)
					} // else: unchanged

					present[g.ID] = true
				}
			}

			for _, ginfo := range old {
				if !present[ginfo.ID()] {
					unassigned = append(unassigned, ginfo.ID())
				}
			}

			if len(fresh) > 0 {
				assigned = append(assigned, model.NewAssignment(consumer.instance, fresh...))
			}
			continue
		} // else: new consumer. Add assignment regardless of any grants

		current := slicex.Map(grants.Allocated, toGrantInfo)
		current = append(current, slicex.Map(grants.Active, toGrantInfo)...)
		current = append(current, slicex.Map(grants.Revoked, toGrantInfo)...)

		assigned = append(assigned, model.NewAssignment(consumer.instance, current...))
	}
	for _, consumer := range c.cluster.Consumers() {
		if _, ok := c.alloc.Worker(consumer.ID()); !ok {
			removed = append(removed, consumer.ID())
		}
	}

	// (2) If anything changed, emit update

	if len(assigned)+len(updated)+len(unassigned)+len(removed) == 0 && !opts.sendShards {
		return
	}

	var copts []model.ClusterChangeOption
	if opts.sendShards {
		copts = append(copts, model.WithClusterChangeShards(opts.shards...))
	}

	change := model.NewClusterChange(c.cluster.ID().Next(c.cl.Now()), assigned, updated, unassigned, removed, copts...)
	upd, err := model.UpdateClusterMap(ctx, c.cluster, change)
	if err != nil {
		log.Errorf(ctx, "Internal: failed to update cluster map: %v", err)
		// TODO (styurin, 7/30/2024): Start drain and exit
	}

	c.cluster = upd

	msg := model.NewClusterMessage(change)
	for _, s := range c.consumers {
		c.mustSend(ctx, s, msg)
	}

	log.Infof(ctx, "Sent cluster update %v/%v: %v", c.name, c.alloc, c.cluster.ID())
}

func (c *coordinator) handleServiceInfoRequest(ctx context.Context) (*internal_v1.CoordinatorOperationResponse, error) {
	consumers, snapshot, err := syncx.Txn2(ctx, c.txn, func() ([]model.Consumer, model.ClusterSnapshot, error) {
		consumers := mapx.MapValues(c.consumers, func(s *consumerSession) model.Consumer {
			return s.consumer.Instance()
		})
		snapshot := model.WrapClusterSnapshot(&public_v1.ClusterMessage_Snapshot{
			Assignments: slicex.Map(c.cluster.Assignments(), model.UnwrapAssignment),
			Origin:      location.UnwrapInstance(c.cluster.ID().Origin),
		})
		return consumers, snapshot, nil
	})
	if err != nil {
		return nil, err
	}
	return &internal_v1.CoordinatorOperationResponse{
		Resp: &internal_v1.CoordinatorOperationResponse_Info{
			Info: &internal_v1.CoordinatorInfoResponse{
				Consumers: slicex.Map(consumers, model.UnwrapInstance),
				Snapshot:  model.UnwrapClusterSnapshot(snapshot),
			},
		},
	}, nil
}

func (c *coordinator) handleServiceRestartRequest(ctx context.Context) (*internal_v1.CoordinatorOperationResponse, error) {
	log.Infof(ctx, "Received restart request for %v, draining the coordinator", c.info.Name())
	c.Drain(20 * time.Second)
	return &internal_v1.CoordinatorOperationResponse{
		Resp: &internal_v1.CoordinatorOperationResponse_Restart{
			Restart: &internal_v1.CoordinatorRestartResponse{},
		},
	}, nil
}

func (c *coordinator) handleConsumerSuspendRequest(ctx context.Context, id model.InstanceID) (*internal_v1.CoordinatorOperationResponse, error) {
	_, err := syncx.Txn1(ctx, c.txn, func() (model.Consumer, error) {
		s, ok := c.consumers[id]
		if !ok {
			return model.Consumer{}, fmt.Errorf("consumer not found, %v: %w", id, model.ErrNotFound)
		}
		if _, ok := c.alloc.Suspend(s.consumer.ID()); !ok {
			return model.Consumer{}, fmt.Errorf("failed to suspend consumer, %v", id)
		}

		log.Infof(ctx, "Suspended consumer: %v", s.consumer.Instance())
		return s.consumer.instance, nil
	})
	if err != nil {
		return nil, err
	}
	return &internal_v1.CoordinatorOperationResponse{
		Resp: &internal_v1.CoordinatorOperationResponse_Suspend{
			Suspend: &internal_v1.CoordinatorConsumerSuspendResponse{},
		},
	}, nil
}

func (c *coordinator) handleConsumerResumeRequest(ctx context.Context, id model.InstanceID) (*internal_v1.CoordinatorOperationResponse, error) {
	_, err := syncx.Txn1(ctx, c.txn, func() (model.Consumer, error) {
		s, ok := c.consumers[id]
		if !ok {
			return model.Consumer{}, fmt.Errorf("consumer not found, %v: %w", id, model.ErrNotFound)
		}
		if _, ok := c.alloc.Resume(s.consumer.ID()); !ok {
			return model.Consumer{}, fmt.Errorf("failed to resume consumer, %v", id)
		}

		log.Infof(ctx, "resumed consumer: %v", s.consumer.Instance())
		return s.consumer.instance, nil
	})
	if err != nil {
		return nil, err
	}
	return &internal_v1.CoordinatorOperationResponse{
		Resp: &internal_v1.CoordinatorOperationResponse_Resume{
			Resume: &internal_v1.CoordinatorConsumerResumeResponse{},
		},
	}, nil
}

func (c *coordinator) handleConsumerDrainRequest(ctx context.Context, id model.InstanceID) (*internal_v1.CoordinatorOperationResponse, error) {
	_, err := syncx.Txn1(ctx, c.txn, func() (model.Consumer, error) {
		s, ok := c.consumers[id]
		if !ok {
			return model.Consumer{}, fmt.Errorf("consumer not found, %v: %w", id, model.ErrNotFound)
		}

		now := c.cl.Now()

		// Revoke assigned grants and release allocated grants
		assigned := c.alloc.Assigned(s.consumer.ID())
		for _, g := range assigned.Allocated {
			c.alloc.Release(g, now) // no promotion
		}
		if len(assigned.Active) > 0 {
			revoked, _ := c.alloc.Revoke(s.consumer.ID(), now, assigned.Active...)
			if !s.TrySend(ctx, model.NewRevoke(slicex.Map(revoked, toGrant)...)) {
				log.Errorf(ctx, "Failed to revoke %v grants for worker: %v. Disconnecting", len(assigned.Active), s)
				c.disconnect(ctx, "stuck", s)
				return model.Consumer{}, fmt.Errorf("failed to revoke grants: %v", revoked)
			}
		}

		// Allocate unassigned grants
		c.allocate(ctx, now, false)

		log.Infof(ctx, "drained consumer: %v", s.consumer.Instance())

		return s.consumer.Instance(), nil
	})
	if err != nil {
		return nil, err
	}
	return &internal_v1.CoordinatorOperationResponse{
		Resp: &internal_v1.CoordinatorOperationResponse_Drain{
			Drain: &internal_v1.CoordinatorConsumerDrainResponse{},
		},
	}, nil
}

// mustSend attempts to send a message on a consumer connection. If it fails, disconnect the consumer
func (c *coordinator) mustSend(ctx context.Context, s *consumerSession, message model.ConsumerMessage) bool {
	if s == nil {
		return false // already disconnected
	}
	if !s.TrySend(ctx, message) {
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
	defer c.recordActionLatency(ctx, "metrics", c.cl.Now())

	c.resetMetrics(ctx)

	// Consumers with status
	numConsumers.Set(ctx, float64(len(c.consumers)), slicex.CopyAppend(core.QualifiedServiceTags(c.name), core.StatusTag("ok"))...)

	// Static shard counts
	for _, domain := range c.cache.Domains(c.name) {
		shards := domain.Config().ShardingPolicy().Shards()
		numShards.Set(ctx, float64(shards), core.QualifiedDomainTags(domain.Name())...)
	}

	// Assigned grants and Load by domain and location
	assigned := map[model.QualifiedDomainName]map[model.GrantState]int{}
	for _, worker := range c.alloc.Workers() {
		assign := c.alloc.Assigned(worker.ID())

		numAssignmentsByLocation.Set(
			ctx,
			float64(len(assign.Active)+len(assign.Revoked)),
			slicex.CopyAppend(
				core.QualifiedServiceTags(c.name),
				core.InstanceIDTag(worker.ID()),
				core.LocationTag(worker.Instance.Data.Instance().Location()))...,
		)

		adj, _ := c.alloc.LoadByWorker(worker.ID())
		numLoadByLocation.Set(
			ctx,
			float64(adj.Load),
			slicex.CopyAppend(
				core.QualifiedServiceTags(c.name),
				core.InstanceIDTag(worker.ID()),
				core.LocationTag(worker.Instance.Data.Instance().Location()))...,
		)
		numPlacementByLocation.Set(
			ctx,
			float64(adj.Place),
			slicex.CopyAppend(
				core.QualifiedServiceTags(c.name),
				core.InstanceIDTag(worker.ID()),
				core.LocationTag(worker.Instance.Data.Instance().Location()))...,
		)
		numColocationByLocation.Set(
			ctx,
			float64(adj.Colo),
			slicex.CopyAppend(
				core.QualifiedServiceTags(c.name),
				core.InstanceIDTag(worker.ID()),
				core.LocationTag(worker.Instance.Data.Instance().Location()))...,
		)

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
			if allocated.Mod == allocation.Loaded {
				assigned[allocated.Unit.Domain][model.LoadedGrantState] += 1
			} else {
				assigned[allocated.Unit.Domain][model.AllocatedGrantState] += 1
			}
		}
		for _, revoked := range assign.Revoked {
			if assigned[revoked.Unit.Domain] == nil {
				assigned[revoked.Unit.Domain] = map[model.GrantState]int{}
			}
			if revoked.Mod == allocation.Unloaded {
				assigned[revoked.Unit.Domain][model.UnloadedGrantState] += 1
			} else {
				assigned[revoked.Unit.Domain][model.RevokedGrantState] += 1
			}
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
	numAssignmentsByLocation.Reset(ctx, core.QualifiedServiceTags(c.name)...)
	numLoadByLocation.Reset(ctx, core.QualifiedServiceTags(c.name)...)
	numPlacementByLocation.Reset(ctx, core.QualifiedServiceTags(c.name)...)
	numColocationByLocation.Reset(ctx, core.QualifiedServiceTags(c.name)...)
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

func (c *coordinator) recordAction(ctx context.Context, action, result string) {
	numActions.Increment(ctx, 1, slicex.CopyAppend(core.QualifiedServiceTags(c.name), core.ActionTag(action), core.ResultTag(result))...)
}

func (c *coordinator) recordActionLatency(ctx context.Context, action string, now time.Time) {
	numActionLatency.Observe(ctx, c.cl.Since(now), slicex.CopyAppend(core.QualifiedServiceTags(c.name), core.ActionTag(action))...)
}
