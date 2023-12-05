package coordinator

import (
	"context"
	"atoms.co/lib-go/pkg/clock"
	"go.atoms.co/splitter/lib/service/location"
	"go.atoms.co/splitter/lib/service/session"
	"go.atoms.co/lib/log"
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

var (
	// leaseDuration is the duration of a consumer lease.
	leaseDuration = 40 * time.Second
)

// Coordinator handles consumer connection and work allocation.
type Coordinator interface {
	iox.AsyncCloser

	Connect(ctx context.Context, sid session.ID, register model.RegisterMessage, in <-chan model.ConsumerMessage) (<-chan model.ConsumerMessage, error)
	Drain(timeout time.Duration)
}

// coordinator is responsible for managing a single tenant. It accepts incoming connections from consumers and
// distributes work among the consumers by assigning shards with leases.
type coordinator struct {
	iox.AsyncCloser

	cl clock.Clock
	id location.Instance

	name  model.QualifiedServiceName
	info  model.ServiceInfoEx
	cache *storage.Cache

	consumers map[model.InstanceID]*consumerSession
	alloc     *Allocation
	cluster   model.Cluster
	messages  chan *sessionx.Message[model.ConsumerMessage]

	inject chan func()

	drain iox.AsyncCloser
}

func New(ctx context.Context, loc location.Location, cl clock.Clock, service model.QualifiedServiceName, state core.State, updates <-chan core.Update) Coordinator {
	c := &coordinator{
		AsyncCloser: iox.WithQuit(ctx.Done(), iox.NewAsyncCloser()),
		id:          location.NewInstance(loc),
		cl:          cl,
		name:        service,
		cache:       storage.NewCache(),
		consumers:   map[model.InstanceID]*consumerSession{},
		messages:    make(chan *sessionx.Message[model.ConsumerMessage], 1000),
		inject:      make(chan func()),
		drain:       iox.NewAsyncCloser(),
	}
	go c.init(core.NewServiceContext(context.Background(), service), state, updates)

	return c
}

func (c *coordinator) Connect(ctx context.Context, sid session.ID, register model.RegisterMessage, in <-chan model.ConsumerMessage) (<-chan model.ConsumerMessage, error) {
	return syncx.Txn1(ctx, c.txn, func() (<-chan model.ConsumerMessage, error) {
		wctx, cancel := contextx.WithQuitCancel(context.Background(), c.Closed())

		s, out, err := c.connect(wctx, sid, register, in)
		if err != nil {
			return nil, err
		}

		c.allocate(ctx, c.cl.Now(), false) // Allocate to assign work post connection

		go func() {
			defer cancel()
			<-s.connection.Closed()

			syncx.AsyncTxn(c.txn, func() {
				if cur, ok := c.consumers[s.consumer.ID()]; ok {
					if ok && cur.connection.Sid() == sid { // guard against race
						c.disconnect(wctx, cur)
					}
				}
			})
		}()
		return out, nil
	})
}

func (c *coordinator) connect(ctx context.Context, sid session.ID, register model.RegisterMessage, in <-chan model.ConsumerMessage) (*consumerSession, <-chan model.ConsumerMessage, error) {
	consumer := NewConsumer(register.Consumer(), c.cl.Now())
	active, err := slicex.TryMap(register.Active(), fromGrant(consumer))
	if err != nil {
		return nil, nil, err
	}

	if old, ok := c.consumers[consumer.ID()]; ok {
		log.Infof(ctx, "Consumer %v re-connected (session=%v) with #grants: %d. Disconnecting stale session", consumer, sid, len(active))
		c.disconnect(ctx, old)
	} else {
		log.Infof(ctx, "Consumer %v connected (session=%v) with #grants: %d", consumer, sid, len(active))
	}

	connection, out := sessionx.NewConnection[model.ConsumerMessage](c.cl, sid, consumer.Instance(), c, in, c.messages)
	s := &consumerSession{
		consumer:   consumer,
		connection: connection,
	}

	lease := c.cl.Now().Add(leaseDuration)
	connection.Send(ctx, model.NewExtend(lease))

	assigned, ok := c.alloc.Attach(allocation.NewWorker(consumer.instance.Client().ID(), consumer.instance), lease, active...)
	if !ok {
		log.Warnf(ctx, "Consumer %v is already attached in the allocation: %d", consumer)
	}

	if len(assigned.Active) > 0 {
		if ok := connection.Send(ctx, model.NewAssign(toGrants(assigned.Active)...)); !ok {
			return nil, nil, fmt.Errorf("broken connection while initializing new consumer %v", consumer)
		}
	}

	if len(assigned.Allocated) > 0 {
		if ok := connection.Send(ctx, model.NewAssign(toGrants(assigned.Allocated)...)); !ok {
			return nil, nil, fmt.Errorf("broken connection while initializing new consumer %v", consumer)
		}
	}

	c.updateCluster(ctx)

	// Send full cluster to the consumer
	if ok := connection.Send(ctx, model.NewClusterSnapshot(c.cluster.ID(), c.cluster.Version(), model.ClusterToAssignments(c.cluster)...)); !ok {
		return nil, nil, fmt.Errorf("broken connection while initializing new consumer %v", consumer)
	}

	c.consumers[consumer.ID()] = s

	return s, out, nil
}

func (c *coordinator) disconnect(ctx context.Context, list ...*consumerSession) {
	for _, s := range list {
		if c.alloc.Detach(s.consumer.ID()) {
			assigned := c.alloc.Assigned(s.ID())
			if len(assigned.Active) > 0 || len(assigned.Allocated) > 0 {
				log.Warnf(ctx, "Detached consumer %v with %v active and %v allocated domains", s.consumer, len(assigned.Active), len(assigned.Allocated))
			}
		} // else: already detached

		s.connection.Close()

		delete(c.consumers, s.consumer.ID())
	}
}

func (c *coordinator) init(ctx context.Context, state core.State, updates <-chan core.Update) {
	defer c.Close()
	defer c.drain.Close()

	c.cache.Restore(core.NewSnapshot(state))

	info, ok := c.cache.Service(c.name)
	if !ok {
		log.Errorf(ctx, "Internal: invalid state for coordinator %v/%v", c.name, c.id)
		return
	}
	c.info = info
	c.alloc = newAllocation(c.id.ID(), info, c.cache.Placements(c.name.Tenant), c.cl.Now().Add(leaseDuration))
	c.cluster, _ = toCluster(c.alloc, model.NewClusterID(), 0)

	log.Infof(ctx, "Coordinator %v/%v initialized, #shards=%v", c.name, c.id, c.alloc.Size())

	c.process(ctx, updates)

	for _, s := range c.consumers {
		s.connection.Disconnect()
	}

	log.Infof(ctx, "Coordinator %v/%v closed", c.name, c.id)
}

func (c *coordinator) process(ctx context.Context, updates <-chan core.Update) {
	ticker := c.cl.NewTicker(10*time.Second + randx.Duration(time.Second))
	defer ticker.Stop()

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

		case update, ok := <-updates:
			if !ok {
				break steady
			}
			if err := c.cache.Update(update, false); err != nil {
				log.Errorf(ctx, "Internal: invalid state update %v", err)
				break steady
			}
			c.refresh(ctx, leaseDuration)

		case <-ticker.C:
			// Regularly send out lease updates to healthy consumers, disconnect unhealthy ones.

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
			c.disconnect(ctx, unhealthy...)
			c.allocate(ctx, now, true)

			// TODO (styurin, 9/29/23): emit metrics

		case fn := <-c.inject:
			fn()

		case <-c.drain.Closed():
			break steady

		case <-c.Closed():
			return
		}
	}

	log.Infof(ctx, "Coordinator %v closing, #consumer=%v", c.id, len(c.consumers))

	// TODO: send out lease update to better survive coordinator restart

	for _, s := range c.consumers {
		s.connection.Close()
	}
}

func (c *coordinator) refresh(ctx context.Context, delay time.Duration) {
	now := c.cl.Now()

	upd, rejected := updateAllocation(c.alloc, c.info, c.cache.Placements(c.name.Tenant), now.Add(delay))
	c.alloc = upd

	for _, g := range rejected {
		c.mustSend(ctx, c.consumers[g.Worker], model.NewRevoke(toGrant(g)))
	}
	c.updateCluster(ctx)
}

func (c *coordinator) allocate(ctx context.Context, now time.Time, loadbalance bool) {
	assign := map[model.ConsumerID][]Grant{}

	// (1) Free any expired grants to make them available for re-allocation.

	promo := c.alloc.Expire(now)
	for _, g := range promo {
		log.Debugf(ctx, "Promoted %v", g)

		assign[g.Worker] = append(assign[g.Worker], g)
	}

	// (2) Allocate and commit the assignments by sending grants. We remove the clients from the alloc
	// on disconnect, so we know that any beneficiary of Allocate has a valid session. However, consumers
	// may be disconnected _during_ grant distribution if stuck.

	grants := c.alloc.Allocate(now)
	for _, g := range grants {
		if _, ok := c.consumers[g.Worker]; !ok {
			c.alloc.Release(g, now) // undo allocation
			continue
		}

		log.Debugf(ctx, "Allocated %v", g)

		assign[g.Worker] = append(assign[g.Worker], g)
	}

	// (4) Load-balance grants across consumers, if needed.

	if loadbalance {
		if move, load, ok := c.alloc.LoadBalance(now); ok {
			assign[move.To.Worker] = append(assign[move.To.Worker], move.To)

			c.mustSend(ctx, c.consumers[move.From.Worker], model.NewRevoke(toGrant(move.From)))

			log.Debugf(ctx, "Initiated grant move: %v, load=%v", move, load)
		}
	}

	// (5) Send out new assignments and broadcast updates, incl revoke

	for cid, grants := range assign {
		c.mustSend(ctx, c.consumers[cid], model.NewAssign(toGrants(grants)...))
	}

	c.updateCluster(ctx)
}

func (c *coordinator) handleConsumerMessage(ctx context.Context, s *consumerSession, m model.ConsumerMessage) {
	msg, ok := m.ClientMessage()
	if !ok {
		log.Errorf(ctx, "Internal: unexpected message: %v", m)
		c.disconnect(ctx, s)
		return
	}

	switch {
	case msg.IsDeregister():
		c.disconnect(ctx, s)

	case msg.IsReleased():
		r, _ := msg.Released()
		grants, err := slicex.TryMap(r.Grants(), fromGrant(s.consumer))
		if err != nil {
			log.Errorf(ctx, "Internal: invalid grants: %v", err)
		}
		c.release(ctx, s, grants)

	default:
		log.Errorf(ctx, "Internal: unexpected consumer message: %v", msg)
	}
}

func (c *coordinator) release(ctx context.Context, s *consumerSession, grants []Grant) {
	for _, grant := range grants {
		c.alloc.Release(grant, c.cl.Now())
	}
	c.updateCluster(ctx)
}

func (c *coordinator) updateCluster(ctx context.Context) {
	newCluster, err := toCluster(c.alloc, c.cluster.ID(), c.cluster.Version()+1)
	if err != nil {
		log.Errorf(ctx, "Internal: unable to create cluster from allocation: %v", err)
		return
	}

	d := newCluster.Diff(c.cluster)
	if d.IsEmpty() {
		return
	}

	assigned := mapx.MapToSlice(d.Assigned, func(cid model.ConsumerID, grants []model.GrantInfo) model.Assignment {
		consumer, _ := newCluster.Consumer(cid)
		return model.NewAssignment(consumer, grants...)
	})
	c.broadcast(ctx, model.NewClusterChange(newCluster.ID(), newCluster.Version(), assigned, d.Updated, d.Unassigned, d.Detached))
	c.cluster = newCluster
	log.Debugf(ctx, "New allocation: %v/%v. Cluster: %v", c.name, c.alloc, newCluster)
}

// mustSend attempts to send a message on a consumer connection. If it fails, disconnect the consumer
func (c *coordinator) mustSend(ctx context.Context, s *consumerSession, message model.ConsumerMessage) bool {
	if s == nil {
		return false // already disconnected
	}
	if !s.connection.Send(ctx, message) {
		c.disconnect(ctx, s)
		return false
	}
	return true
}

func (c *coordinator) broadcast(ctx context.Context, message model.ConsumerMessage) {
	for _, s := range c.consumers {
		c.mustSend(ctx, s, message)
	}
}

func (c *coordinator) Drain(timeout time.Duration) {
	c.drain.Close()
	c.cl.AfterFunc(timeout, c.Close)
}

func (c *coordinator) String() string {
	return fmt.Sprintf("%v[alloc=%v, #consumers=%v]", c.name, c.alloc, len(c.consumers))
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
