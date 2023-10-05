package coordinator

import (
	"context"
	"atoms.co/lib-go/pkg/clock"
	"go.atoms.co/splitter/lib/service/session"
	"go.atoms.co/lib/log"
	"go.atoms.co/lib/iox"
	"go.atoms.co/lib/randx"
	"go.atoms.co/lib/syncx"
	"go.atoms.co/splitter/pkg/core"
	"go.atoms.co/splitter/pkg/model"
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

	state    core.State
	shards   *ShardManager
	sessions map[session.ID]*consumerSession
	in       chan consumerMessage
}

func New(ctx context.Context, cl clock.Clock, tenant model.TenantName, state core.State, stateUpdates <-chan core.Update) *Coordinator {
	c := &Coordinator{
		AsyncCloser: iox.WithQuit(ctx.Done(), iox.NewAsyncCloser()),
		cl:          cl,
		inject:      make(chan func()),
		drain:       iox.NewAsyncCloser(),

		tenant: tenant,

		state:    state,
		shards:   NewShardManager(ctx, cl, state),
		sessions: map[session.ID]*consumerSession{},
		in:       make(chan consumerMessage, 1000),
	}
	ctx = log.NewContext(ctx, log.String("tenant", string(tenant)))
	go c.process(ctx, stateUpdates)
	return c
}

func (c *Coordinator) Connect(ctx context.Context, sid session.ID, register model.RegisterMessage, in <-chan model.ConsumerMessage) (<-chan model.ConsumerMessage, error) {
	var snapshot model.ClusterSnapshot
	consumer, err := syncx.Txn1(ctx, txnx.Txn(c, c.inject), func() (*consumerSession, error) {
		s, ok := c.sessions[sid]
		if ok {
			s.Close()
		}
		consumer := NewConsumer(sid, register.Instance(), c.cl.Now())

		err := c.shards.Connect(consumer, 1, c.cl.Now().Add(time.Minute*10))
		if err != nil {
			log.Errorf(ctx, "Coordination %v is unable to connect a consumer %v: %v", c, consumer, err)
			return nil, err
		}

		s = newConsumerSession(c.cl, c.Closed(), sid, consumer)

		go func() {
			select {
			case <-c.Closed():
				s.Close()
			case <-s.Closed():
				_ = c.shards.Disconnect(consumer)
			}
		}()

		c.sessions[sid] = s
		log.Infof(ctx, "%v connected consumer %v", c, s)
		snapshot = c.shards.Snapshot()
		return s, nil
	})
	if err != nil {
		return nil, err
	}
	if !consumer.trySend(ctx, model.NewConsumerClusterMessage(model.NewClusterSnapshotMessage(snapshot))) {
		return nil, fmt.Errorf("unable to send initial cluster message to consumer %v", register)
	}
	go c.forward(ctx, in, consumer)
	return consumer.out, nil
}

func (c *Coordinator) Drain(timeout time.Duration) {
	c.drain.Close()
	c.cl.AfterFunc(timeout, c.Close)
}

func (c *Coordinator) String() string {
	return fmt.Sprintf("coordinator{tenant=%v}", c.tenant)
}

func (c *Coordinator) forward(ctx context.Context, in <-chan model.ConsumerMessage, consumer *consumerSession) {
	defer consumer.Close()
	for {
		select {
		case msg, ok := <-in:
			if !ok {
				return
			}
			select {
			case c.in <- newConsumerMessage(consumer.sid, msg):
			case <-c.Closed():
				log.Warnf(ctx, "Coordinator %v is closed. Dropping message %v", c, msg)
				return
			}
		case <-c.Closed():
			return
		}
	}
}

func (c *Coordinator) process(ctx context.Context, stateUpdates <-chan core.Update) {
	defer c.Close()

	ticker := c.cl.NewTicker(10*time.Second + randx.Duration(time.Second))
	defer ticker.Stop()

	for {
		select {
		case msg := <-c.in:
			c.handleConsumerMessage(ctx, msg.sid, msg.msg)
		case <-ticker.C:
			update, ok := c.shards.DiscardClusterUpdate(ctx)
			if ok {
				for _, s := range c.sessions {
					if !s.trySend(ctx, model.NewConsumerClusterMessage(model.NewClusterUpdateMessage(update))) {
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

func (c *Coordinator) handleConsumerMessage(ctx context.Context, sid session.ID, msg model.ConsumerMessage) {
	s, ok := c.sessions[sid]
	if !ok {
		log.Errorf(ctx, "Unable to find session for message %v", msg)
		return
	}
	switch {
	case msg.IsDeregister():
		log.Infof(ctx, "Coordinator %v is disconnecting session %v", c, s)
		c.tearDown(ctx, sid)
	case msg.IsReleased():
		released, _ := msg.Released()
		c.handleReleased(ctx, s, released)
	}
}

func (c *Coordinator) handleReleased(ctx context.Context, consumer *consumerSession, msg model.ReleasedMessage) {
	grants, err := msg.ParseGrants()
	if err != nil {
		log.Errorf(ctx, "Unable to parse grants from %v: %v", msg, err)
		return
	}
	for _, g := range grants {
		err := c.shards.Release(consumer.consumer, g)
		if err != nil {
			log.Errorf(ctx, "Coordinator %v is unable to release a grant %v for consumer %v: %v", c, g, consumer, err)
			continue
		}
	}
	log.Infof(ctx, "Coordinator %v released %v grants from consumer %s", c, len(grants), consumer)
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

func (c *Coordinator) tearDown(ctx context.Context, sid session.ID) {
	s, ok := c.sessions[sid]
	if !ok {
		return
	}
	log.Infof(ctx, "Coordinator %v is disconnecting session %v", c, s)
	delete(c.sessions, sid)
	s.Close()
}

type consumerSession struct {
	iox.AsyncCloser

	cl clock.Clock

	sid      session.ID
	consumer Consumer

	out chan model.ConsumerMessage
}

func newConsumerSession(cl clock.Clock, quit <-chan struct{}, sid session.ID, consumer Consumer) *consumerSession {
	return &consumerSession{
		AsyncCloser: iox.WithQuit(quit, iox.NewAsyncCloser()), // quit => closer
		cl:          cl,
		sid:         sid,
		consumer:    consumer,
		out:         make(chan model.ConsumerMessage, 100),
	}
}

func (c *consumerSession) trySend(ctx context.Context, msg model.ConsumerMessage) bool {
	timer := c.cl.NewTimer(5 * time.Second)
	defer timer.Stop()

	select {
	case c.out <- msg:
		return true
	case <-c.Closed():
		return false
	case <-timer.C:
		log.Errorf(ctx, "Consumer connection %v is stuck. Closing", c)
		c.Close()
		return false
	}
}

func (c *consumerSession) String() string {
	return fmt.Sprintf("session{id=%v, consumer=%v}", c.sid, c.consumer)
}

type consumerMessage struct {
	sid session.ID
	msg model.ConsumerMessage
}

func newConsumerMessage(sid session.ID, msg model.ConsumerMessage) consumerMessage {
	return consumerMessage{sid: sid, msg: msg}
}
