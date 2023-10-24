package coordinator

import (
	"context"
	"atoms.co/lib-go/pkg/clock"
	"go.atoms.co/splitter/lib/service/session"
	"go.atoms.co/lib/log"
	"go.atoms.co/lib/contextx"
	"go.atoms.co/lib/iox"
	"go.atoms.co/lib/randx"
	"go.atoms.co/lib/syncx"
	"go.atoms.co/splitter/pkg/core"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pkg/util/sessionx"
	"go.atoms.co/splitter/pkg/util/txnx"
	"fmt"
	"time"
)

var (
	// leaseDuration is the duration of a consumer lease.
	leaseDuration = 90 * time.Second
)

// Coordinator is responsible for managing a single tenant. It accepts incoming connections from consumers and
// distributes work among the consumers by assigning shards with leases.
type Coordinator struct {
	iox.AsyncCloser

	cl     clock.Clock
	inject chan func()
	drain  iox.AsyncCloser

	tenant model.TenantName

	state     core.State
	shards    *ShardManager
	consumers map[model.InstanceID]*consumerSession
	messages  chan *sessionx.Message[model.ConsumerMessage]
}

type consumerSession struct {
	consumer   Consumer
	connection sessionx.Connection[model.ConsumerMessage]
}

func (c *consumerSession) String() string {
	return fmt.Sprintf("session{consumer=%v, connection=%v}", c.consumer, c.connection)
}

func New(ctx context.Context, cl clock.Clock, tenant model.TenantName, state core.State, stateUpdates <-chan core.Update) *Coordinator {
	c := &Coordinator{
		AsyncCloser: iox.WithQuit(ctx.Done(), iox.NewAsyncCloser()),
		cl:          cl,
		inject:      make(chan func()),
		drain:       iox.NewAsyncCloser(),
		tenant:      tenant,
		state:       state,
		shards:      NewShardManager(ctx, cl, state),
		consumers:   map[model.InstanceID]*consumerSession{},
		messages:    make(chan *sessionx.Message[model.ConsumerMessage], 1000),
	}
	ctx = log.NewContext(ctx, log.String("tenant", string(tenant)))
	go c.process(ctx, stateUpdates)
	return c
}

func (c *Coordinator) Connect(ctx context.Context, sid session.ID, register model.RegisterMessage, in <-chan model.ConsumerMessage) (<-chan model.ConsumerMessage, error) {
	out, err := syncx.Txn1(ctx, txnx.Txn(c, c.inject), func() (<-chan model.ConsumerMessage, error) {
		id := register.Instance().ID()
		existing, ok := c.consumers[id]
		if ok {
			existing.connection.Close()
		}

		wctx, cancel := contextx.WithQuitCancel(context.Background(), c.Closed())
		s, out, err := c.connectConsumer(wctx, sid, register, in)
		if err != nil {
			return nil, err
		}

		go func() {
			defer cancel()
			<-s.connection.Closed()

			syncx.AsyncTxn(txnx.Txn(c, c.inject), func() {
				if cur, ok := c.consumers[id]; ok {
					if ok && cur.connection.Sid() == sid { // guard against race
						c.disconnectConsumer(wctx, cur)
					}
				}
			})
		}()
		return out, nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *Coordinator) Drain(timeout time.Duration) {
	c.drain.Close()
	c.cl.AfterFunc(timeout, c.Close)
}

func (c *Coordinator) String() string {
	return fmt.Sprintf("coordinator{tenant=%v}", c.tenant)
}

func (c *Coordinator) connectConsumer(ctx context.Context, sid session.ID, register model.RegisterMessage, in <-chan model.ConsumerMessage) (*consumerSession, <-chan model.ConsumerMessage, error) {
	consumer := NewConsumer(register.Instance(), c.cl.Now())
	err := c.shards.Connect(consumer, 1, c.cl.Now().Add(time.Minute*10))
	if err != nil {
		log.Errorf(ctx, "Coordination %v is unable to connect a consumer %v: %v", c, consumer, err)
		return nil, nil, err
	}
	connection, out := sessionx.NewConnection[model.ConsumerMessage](c.cl, sid, consumer.Instance(), c, in, c.messages)
	c.consumers[consumer.ID()] = &consumerSession{
		consumer:   consumer,
		connection: connection,
	}

	log.Infof(ctx, "%v connected consumer %v", c, consumer)
	snapshot := c.shards.Snapshot()

	if !connection.Send(ctx, model.NewConsumerClusterMessage(model.NewClusterSnapshotMessage(snapshot))) {
		return nil, nil, fmt.Errorf("unable to send initial cluster message to consumer %v", register)
	}
	return c.consumers[consumer.ID()], out, nil
}

func (c *Coordinator) disconnectConsumer(ctx context.Context, s *consumerSession) {
	delete(c.consumers, s.consumer.ID())
	err := c.shards.Disconnect(s.consumer)
	if err != nil {
		log.Errorf(ctx, "Error when disconnecting a consumer %v from allocations: %v", s.consumer, err)
	}
}

func (c *Coordinator) process(ctx context.Context, stateUpdates <-chan core.Update) {
	defer c.Close()

	ticker := c.cl.NewTicker(10*time.Second + randx.Duration(time.Second))
	defer ticker.Stop()

	for {
		select {
		case msg := <-c.messages:
			s, ok := c.consumers[msg.Instance.ID()]
			if !ok || msg.Sid != s.connection.Sid() {
				log.Debugf(ctx, "Ignoring stale message from consumer session %v: %v", msg.Sid, msg)
				break
			}
			c.handleConsumerMessage(ctx, s, msg.Msg)
		case <-ticker.C:
			update, ok := c.shards.DiscardClusterUpdate(ctx)
			if ok {
				for _, s := range c.consumers {
					if !s.connection.Send(ctx, model.NewConsumerClusterMessage(model.NewClusterUpdateMessage(update))) {
						log.Errorf(ctx, "Cannot send cluster updates to session %v", s)
					}
				}
			}
			// TODO (styurin, 9/29/23): emit metrics
		case fn := <-c.inject:
			fn()
		case update, ok := <-stateUpdates:
			if !ok {
				return
			}
			c.handleStateUpdate(ctx, update)
		case <-c.drain.Closed():
			// drain
		case <-c.Closed():
			return
		}
	}
}

func (c *Coordinator) handleConsumerMessage(ctx context.Context, s *consumerSession, msg model.ConsumerMessage) {
	switch {
	case msg.IsDeregister():
		log.Infof(ctx, "Coordinator %v is disconnecting session %v", c, s)
		c.tearDown(ctx, s.consumer.ID())
	case msg.IsReleased():
		released, _ := msg.Released()
		c.handleReleased(ctx, s, released)
	}
}

func (c *Coordinator) handleReleased(ctx context.Context, s *consumerSession, msg model.ReleasedMessage) {
	grants, err := msg.ParseGrants()
	if err != nil {
		log.Errorf(ctx, "Unable to parse grants from %v: %v", msg, err)
		return
	}
	for _, g := range grants {
		err := c.shards.Release(s.consumer, g)
		if err != nil {
			log.Errorf(ctx, "Coordinator %v is unable to release a grant %v for consumer %v: %v", c, g, s.consumer, err)
			continue
		}
	}
	log.Infof(ctx, "Coordinator %v released %v grants from consumer %s", c, len(grants), s.consumer)
}

func (c *Coordinator) handleStateUpdate(ctx context.Context, update core.Update) {
	if c.tenant != update.Name() {
		log.Errorf(ctx, "%v received an update for mismatching tenant: %v", c, update)
	}
	if upd, ok := update.TenantUpdated(); ok {
		c.handleTenantUpdated(ctx, upd)
	}
	if upd := update.DomainsUpdated(); len(upd) > 0 {
		c.handleDomainUpdated(ctx, upd)
	}
	if upd := update.DomainsRemoved(); len(upd) > 0 {
		c.handleDomainRemoved(ctx, upd)
	}
	if upd := update.PlacementsUpdated(); len(upd) > 0 {
		c.handlePlacementsUpdated(ctx, upd)
	}
	if upd := update.PlacementsRemoved(); len(upd) > 0 {
		c.handlePlacementsRemoved(ctx, upd)
	}
}

func (c *Coordinator) handleTenantUpdated(ctx context.Context, info model.TenantInfo) {
	log.Debugf(ctx, "%v received tenant update: %v", c, info)
	// TODO: detect if default region and default sharding policy are different and apply the changes
}

func (c *Coordinator) handleDomainUpdated(ctx context.Context, updated []model.DomainInfo) {
	log.Debugf(ctx, "%v received domain updates: %v", c, updated)
	// TODO: detect if a domain is added and add new shards to the distribution
	// TODO: detect if domain's state has changed and add/remove shards
	// TODO: detect if config has changed and update affected shards
}

func (c *Coordinator) handleDomainRemoved(ctx context.Context, removed []model.QualifiedDomainName) {
	log.Debugf(ctx, "%v received domain removals: %v", c, removed)
	// TODO: remove shards of removed domain
}

func (c *Coordinator) handlePlacementsUpdated(ctx context.Context, updated []core.InternalPlacementInfo) {
	log.Debugf(ctx, "%v received placement updates: %v", c, updated)
	// TODO: update shards affected by updated placements
}

func (c *Coordinator) handlePlacementsRemoved(ctx context.Context, removed []model.QualifiedPlacementName) {
	log.Debugf(ctx, "%v received placement removals: %v", c, removed)
	// TODO: update shards affected by removed placements
}

func (c *Coordinator) notifyConsumers() {
	// TODO: send cluster updates to consumers
}

func (c *Coordinator) tearDown(ctx context.Context, id model.InstanceID) {
	s, ok := c.consumers[id]
	if !ok {
		return
	}
	log.Infof(ctx, "Coordinator %v is disconnecting session %v", c, s)
	delete(c.consumers, id)
	s.connection.Close()
}
