package worker

import (
	"context"
	"atoms.co/lib-go/pkg/clock"
	"go.atoms.co/lib/log"
	"go.atoms.co/lib/iox"
	"go.atoms.co/lib/syncx"
	"go.atoms.co/splitter/pkg/core"
	"go.atoms.co/splitter/pkg/service/coordinator"
	"go.atoms.co/splitter/pkg/util/txnx"
	"time"
)

const (
	statsDuration = 15 * time.Second
)

type CoordinatorFactory func(ctx context.Context) *coordinator.Coordinator

type Worker struct {
	iox.AsyncCloser

	cl     clock.Clock
	fn     CoordinatorFactory
	inject chan func()
	drain  iox.AsyncCloser
}

func New(cl clock.Clock, in <-chan core.WorkerMessage, fn CoordinatorFactory) *Worker {
	w := &Worker{
		AsyncCloser: iox.NewAsyncCloser(),
		cl:          cl,
		fn:          fn,
		inject:      make(chan func()),
		drain:       iox.NewAsyncCloser(),
	}
	go w.process(context.Background(), in)
	return w
}

func (w *Worker) Connect(ctx context.Context) {
	syncx.Txn0(ctx, txnx.Txn(w, w.inject), func() {
		log.Infof(ctx, "connected!")
	})
}

func (w *Worker) Drain(timeout time.Duration) {
	w.drain.Close()
	w.cl.AfterFunc(timeout, w.Close)
}

func (w *Worker) process(ctx context.Context, in <-chan core.WorkerMessage) {
	defer w.Close()

	statsTimer := w.cl.NewTicker(statsDuration)
	defer statsTimer.Stop()

	for {
		select {
		case msg, ok := <-in:
			if !ok {
				return
			}
			handleWorkerMessage(ctx, msg)
		case fn := <-w.inject:
			fn()
		case <-statsTimer.C:
			// record metrics
		case <-w.drain.Closed():
			// deregister from leader
		case <-w.Closed():
			return
		}
	}
}

func handleWorkerMessage(ctx context.Context, msg core.WorkerMessage) {
	switch {
	case msg.IsAssign():
		// create coordinators
	case msg.IsRevoke():
		// drain coordinators / exit on full drain
	case msg.IsDisconnect():
		// reconnect to leader
	case msg.IsLeaseUpdate():
		// extend worker lease
	default:
		log.Errorf(ctx, "Invalid worker message: %v", msg)
	}
}
