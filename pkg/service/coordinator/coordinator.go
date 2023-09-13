package coordinator

import (
	"context"
	"atoms.co/lib-go/pkg/clock"
	"go.atoms.co/lib/log"
	"go.atoms.co/lib/iox"
	"go.atoms.co/lib/syncx"
	"go.atoms.co/splitter/pkg/model"
	"sync"
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
	syncx.Txn0(ctx, c.txn, func() {
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

// txn runs the given function in the main thread sync. Any signal that triggers a complex action must
// perform I/O or expensive parts outside txn and potentially use multiple txn calls.
func (c *Coordinator) txn(ctx context.Context, fn func() error) error {
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
