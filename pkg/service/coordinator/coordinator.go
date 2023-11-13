package coordinator

import (
	"context"
	"atoms.co/lib-go/pkg/clock"
	"go.atoms.co/splitter/lib/service/location"
	"go.atoms.co/splitter/lib/service/session"
	"go.atoms.co/lib/log"
	"go.atoms.co/lib/contextx"
	"go.atoms.co/lib/iox"
	"go.atoms.co/lib/randx"
	"go.atoms.co/lib/syncx"
	"go.atoms.co/splitter/pkg/allocation"
	"go.atoms.co/splitter/pkg/core"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pkg/storage"
	"go.atoms.co/splitter/pkg/util/sessionx"
	"go.atoms.co/splitter/pkg/util/txnx"
	"fmt"
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
	return syncx.Txn1(ctx, txnx.Txn(c, c.inject), func() (<-chan model.ConsumerMessage, error) {
		wctx, cancel := contextx.WithQuitCancel(context.Background(), c.Closed())

		s, out, err := c.connect(wctx, sid, register, in)
		if err != nil {
			return nil, err
		}

		c.allocate(ctx, c.cl.Now(), false) // Allocate to assign work post connection

		go func() {
			defer cancel()
			<-s.connection.Closed()

			syncx.AsyncTxn(txnx.Txn(c, c.inject), func() {
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
	grants, err := register.ParseActive()
	if err != nil {
		return nil, nil, err
	}

	if old, ok := c.consumers[consumer.ID()]; ok {
		log.Infof(ctx, "Consumer %v re-connected (session=%v) with #grants: %d. Disconnecting stale session", consumer, sid, len(grants))
		c.disconnect(ctx, old)
	} else {
		log.Infof(ctx, "Consumer %v connected (session=%v) with #grants: %d", consumer, sid, len(grants))
	}

	connection, out := sessionx.NewConnection[model.ConsumerMessage](c.cl, sid, consumer.Instance(), c, in, c.messages)
	s := &consumerSession{
		consumer:   consumer,
		connection: connection,
	}
	c.consumers[consumer.ID()] = s

	lease := c.cl.Now().Add(leaseDuration)
	connection.Send(ctx, model.NewConsumerExtendMessage(model.NewExtendMessage(lease)))

	// If consumer is present in the allocation, there is no need to broadcast assignments to other
	// consumers. Attach will change no assignments.

	_, broadcast := c.alloc.Worker(consumer.ID())
	if assigned, ok := c.alloc.Attach(allocation.NewWorker(consumer.instance.Client()), lease /* + claimed grants */); ok {
		connection.Send(ctx, model.NewConsumerAssignMessage(model.NewAssignMessage(toGrants(assigned.Active)...)))
		connection.Send(ctx, model.NewConsumerAssignMessage(model.NewAssignMessage(toGrants(assigned.Allocated)...)))
	}

	// TODO: send existing cluster map to new consumer

	if broadcast {
		// TODO
	}

	return s, out, nil
}

func (c *coordinator) disconnect(ctx context.Context, list ...*consumerSession) {
	for _, s := range list {
		if c.alloc.Detach(s.consumer.ID()) {
			assigned := c.alloc.Assigned(s.ID())
			if len(assigned.Active) > 0 || len(assigned.Allocated) > 0 {
				log.Warnf(ctx, "Detached consumer %v with %v active and %v allocated domains: %v ", s.consumer, len(assigned.Active), len(assigned.Allocated))
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

	log.Infof(ctx, "Coordinator %v/%v initialized, #shards=%v", c.name, c.id, c.alloc.Size())

	c.process(ctx, updates)

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
				return
			}
			c.refresh(ctx, leaseDuration)

		case <-ticker.C:
			// Regularly send out lease updates to healthy consumers, disconnect unhealthy ones.

			now := c.cl.Now()
			lease := now.Add(leaseDuration)

			var unhealthy []*consumerSession
			for _, s := range c.consumers {
				if !s.connection.Send(ctx, model.NewConsumerExtendMessage(model.NewExtendMessage(lease))) {
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
		c.mustSend(ctx, c.consumers[g.Worker], model.NewConsumerRevokeMessage(model.NewRevokeMessage(model.GrantID(g.ID))))
	}
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

			c.mustSend(ctx, c.consumers[move.From.Worker], model.NewConsumerRevokeMessage(model.NewRevokeMessage(model.GrantID(move.From.ID))))

			log.Debugf(ctx, "Initiated grant move: %v, load=%v", move, load)
		}
	}

	// (5) Send out new assignments and broadcast updates, incl revoke

	for cid, grants := range assign {
		c.mustSend(ctx, c.consumers[cid], model.NewConsumerAssignMessage(model.NewAssignMessage(toGrants(grants)...)))
	}

	log.Debugf(ctx, "Allocation: %v/%v", c.name, c.alloc)

	// TODO: broadcast
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
func (c *coordinator) handleConsumerMessage(ctx context.Context, s *consumerSession, msg model.ConsumerMessage) {
	switch {
	case msg.IsDeregister():
		c.disconnect(ctx, s)

	default:
		log.Errorf(ctx, "Internal: unexpected consumer message: %v", msg)
	}
}

func (c *coordinator) Drain(timeout time.Duration) {
	c.drain.Close()
	c.cl.AfterFunc(timeout, c.Close)
}

func (c *coordinator) String() string {
	return fmt.Sprintf("%v[alloc=%v, #consumers=%v]", c.name, c.alloc, len(c.consumers))
}
