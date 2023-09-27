package coordinator

import (
	"context"
	"atoms.co/lib-go/pkg/clock"
	"go.atoms.co/splitter/lib/service/location"
	"go.atoms.co/splitter/lib/service/session"
	"go.atoms.co/lib/log"
	"go.atoms.co/lib/iox"
	"go.atoms.co/lib/randx"
	"go.atoms.co/lib/syncx"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pkg/util/txnx"
	"fmt"
	"time"
)

// Coordinator is responsible for managing a single tenant. It accepts incoming connections from consumers and
// distributes work among the consumers by assigning shards with leases.
type Coordinator struct {
	iox.AsyncCloser

	cl     clock.Clock
	inject chan func()
	drain  iox.AsyncCloser

	tenant model.Tenant

	consumers map[session.ID]*consumerSession
	in        chan model.ConsumerMessage
}

func New(ctx context.Context, cl clock.Clock, tenant model.Tenant) *Coordinator {
	c := &Coordinator{
		AsyncCloser: iox.WithQuit(ctx.Done(), iox.NewAsyncCloser()),
		cl:          cl,
		inject:      make(chan func()),
		drain:       iox.NewAsyncCloser(),

		tenant: tenant,

		consumers: map[session.ID]*consumerSession{},
		in:        make(chan model.ConsumerMessage, 1000),
	}
	go c.process(context.Background())
	return c
}

func (c *Coordinator) Connect(ctx context.Context, sid session.ID, register model.RegisterMessage, in <-chan model.ConsumerMessage) (<-chan model.CoordinatorMessage, error) {
	var consumer *consumerSession
	syncx.Txn0(ctx, txnx.Txn(c, c.inject), func() {
		var ok bool
		consumer, ok = c.consumers[sid]
		if ok {
			consumer.disconnect(ctx)
		}
		consumer = newConsumerSession(c.cl, c.Closed(), sid, register.Instance())
		c.consumers[sid] = consumer
		log.Infof(ctx, "%v connected consumer %v", c, consumer)
	})
	c.forward(ctx, in, consumer)
	return consumer.out, nil
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
			case c.in <- msg:
			case <-c.Closed():
				log.Warnf(ctx, "Coordinator %v is closed. Dropping message %v", c, msg)
				return
			}
		case <-c.Closed():
			return
		}
	}
}

func (c *Coordinator) Drain(timeout time.Duration) {
	c.drain.Close()
	c.cl.AfterFunc(timeout, c.Close)
}

func (c *Coordinator) process(ctx context.Context) {
	defer c.Close()

	ticker := c.cl.NewTicker(10*time.Second + randx.Duration(time.Second))
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			log.Infof(ctx, "Coordinator %v is disconnecting all consumers", c)
			for sid := range c.consumers {
				c.tearDown(ctx, sid)
			}
		case fn := <-c.inject:
			fn()
		case <-c.drain.Closed():
			// drain
		case <-c.Closed():
			return
		}
	}
}

func (c *Coordinator) tearDown(ctx context.Context, sid session.ID) {
	consumer, ok := c.consumers[sid]
	if !ok {
		return
	}
	log.Infof(ctx, "Coordinator %v is disconnecting consumer %v", c, consumer)
	consumer.disconnect(ctx)
	delete(c.consumers, sid)
	consumer.Close()
}

type consumerSession struct {
	iox.AsyncCloser

	cl clock.Clock

	sid      session.ID
	location location.Instance

	out chan model.CoordinatorMessage
}

func newConsumerSession(cl clock.Clock, quit <-chan struct{}, sid session.ID, loc location.Instance) *consumerSession {
	return &consumerSession{
		AsyncCloser: iox.WithQuit(quit, iox.NewAsyncCloser()), // quit => closer
		cl:          cl,
		sid:         sid,
		location:    loc,
		out:         make(chan model.CoordinatorMessage, 100),
	}
}

func (c *consumerSession) disconnect(ctx context.Context) {
	if !c.trySend(ctx, model.NewCoordinatorDisconnectMessage()) {
		log.Infof(ctx, "Unable to disconnect consumer session %v", c)
	}
}

func (c *consumerSession) trySend(ctx context.Context, msg model.CoordinatorMessage) bool {
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
	return fmt.Sprintf("consumer{session=%v, location=%v}", c.sid, c.location)
}
