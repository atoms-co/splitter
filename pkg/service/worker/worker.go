package worker

import (
	"context"
	"atoms.co/lib-go/pkg/clock"
	"go.atoms.co/splitter/lib/service/session"
	"go.atoms.co/lib/log"
	"go.atoms.co/lib/iox"
	"go.atoms.co/lib/syncx"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pkg/service/coordinator"
	"go.atoms.co/splitter/pkg/util/txnx"
	"fmt"
	"time"
)

const (
	statsDuration = 15 * time.Second
)

type CoordinatorFactory func(ctx context.Context, tenant model.TenantName) (*coordinator.Coordinator, error)

type Worker struct {
	iox.AsyncCloser

	cl     clock.Clock
	fn     CoordinatorFactory
	inject chan func()
	drain  iox.AsyncCloser

	coordinators map[model.TenantName]*coordinatorInfo
}

func New(cl clock.Clock, in <-chan WorkerMessage, fn CoordinatorFactory) *Worker {
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

// Connect handles connection of a consumer to a local coordinator
func (w *Worker) Connect(ctx context.Context, sid session.ID, register model.RegisterMessage, in <-chan model.ConsumerMessage) (<-chan model.ConsumerMessage, error) {
	tenant := register.TenantName()

	ci, err := syncx.Txn1(ctx, txnx.Txn(w, w.inject), func() (*coordinatorInfo, error) {
		ci, ok := w.coordinators[tenant]
		if !ok {
			return nil, fmt.Errorf("%w: coordinator for tenant %v", model.ErrNotFound, tenant)
		}
		return ci, nil
	})
	if err != nil {
		return nil, err
	}
	return ci.c.Connect(ctx, sid, register, in)
}

func (w *Worker) Drain(timeout time.Duration) {
	w.drain.Close()
	w.cl.AfterFunc(timeout, w.Close)
}

func (w *Worker) process(ctx context.Context, in <-chan WorkerMessage) {
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

func handleWorkerMessage(ctx context.Context, msg WorkerMessage) {
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

type coordinatorInfo struct {
	c *coordinator.Coordinator
}
