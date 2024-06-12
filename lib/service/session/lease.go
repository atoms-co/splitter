package session

import (
	"fmt"
	"sync"
	"time"

	"go.atoms.co/lib/iox"
)

// Lease represent a lease, i.e., time-limited ownership of some associated entity.
type Lease interface {
	// Expiration returns the expiration time.
	Expiration() time.Time
}

// ExtendableLease is an extendable Lease. Thread-safe.
type ExtendableLease struct {
	drainer iox.AsyncCloser
	exp     time.Time
	mu      sync.Mutex
}

func NewExtendableLease(drainer iox.AsyncCloser, exp time.Time) *ExtendableLease {
	return &ExtendableLease{
		drainer: drainer,
		exp:     exp,
	}
}

// Extend extends the lease. A nop if it would shorten it.
func (w *ExtendableLease) Extend(t time.Time) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if t.Before(w.exp) {
		return // ignore: cannot shorten lease
	}
	w.exp = t
}

func (w *ExtendableLease) Expiration() time.Time {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.exp
}

func (w *ExtendableLease) IsExpired(now time.Time) bool {
	w.mu.Lock()
	defer w.mu.Unlock()

	return !now.Before(w.exp)
}

func (w *ExtendableLease) Drained() <-chan struct{} {
	return w.drainer.Closed()
}

func (w *ExtendableLease) String() string {
	return fmt.Sprintf("%v", w.Expiration())
}
