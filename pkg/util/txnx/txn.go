package txnx

import (
	"context"
	"go.atoms.co/lib/iox"
	"go.atoms.co/lib/syncx"
	"go.atoms.co/splitter/pkg/model"
	"sync"
)

// Txn is a helper that constructs a syncx.TxnFn with the project specific error codes and injection channels
func Txn(closer iox.AsyncCloser, inject chan<- func()) syncx.TxnFn {
	return func(ctx context.Context, fn func() error) error {
		var wg sync.WaitGroup
		var err error

		wg.Add(1)
		select {
		case inject <- func() {
			defer wg.Done()
			err = fn()
		}:
			wg.Wait()
			return err
		case <-ctx.Done():
			return model.ErrOverloaded
		case <-closer.Closed():
			return model.ErrDraining
		}
	}
}
