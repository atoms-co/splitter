package coordinator

import (
	"context"
	"atoms.co/lib-go/pkg/clock"
	"go.atoms.co/lib/log"
	"go.atoms.co/lib/iox"
	"go.atoms.co/lib/syncx"
	"go.atoms.co/splitter/pkg/util/txnx"
	"time"
)

type Coordinator struct {
	iox.AsyncCloser

	cl     clock.Clock
	inject chan func()
	drain  iox.AsyncCloser
}

func New(ctx context.Context, cl clock.Clock) *Coordinator {
	c := &Coordinator{
		AsyncCloser: iox.WithQuit(ctx.Done(), iox.NewAsyncCloser()),
		cl:          cl,
		inject:      make(chan func()),
		drain:       iox.NewAsyncCloser(),
	}
	go c.process(context.Background())
	return c
}

func (c *Coordinator) Connect(ctx context.Context) {
	syncx.Txn0(ctx, txnx.Txn(c, c.inject), func() {
		log.Infof(ctx, "connected!")
	})
}

func (c *Coordinator) Drain(timeout time.Duration) {
	c.drain.Close()
	c.cl.AfterFunc(timeout, c.Close)
}

func (c *Coordinator) process(ctx context.Context) {
	defer c.Close()

	for {
		select {
		case fn := <-c.inject:
			fn()
		case <-c.drain.Closed():
			// drain
		case <-c.Closed():
			return
		}
	}
}
