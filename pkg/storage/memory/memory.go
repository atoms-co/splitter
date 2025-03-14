package memory

import (
	"context"
	"sync"

	"go.atoms.co/splitter/pkg/core"
	"go.atoms.co/splitter/pkg/storage"
)

// Storage is an in-memory storage implementation. Useful for single-node setups and testing. Thread-safe.
type Storage struct {
	db *storage.Cache
	mu sync.Mutex
}

func New() storage.Storage {
	return &Storage{
		db: storage.NewCache(),
	}
}

func (s *Storage) Read(ctx context.Context) (core.Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.db.Snapshot(), nil
}

func (s *Storage) Update(ctx context.Context, update core.Update) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.db.Update(update, false)
}

func (s *Storage) Delete(ctx context.Context, del core.Delete) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.db.Delete(del)
}

func (s *Storage) Restore(ctx context.Context, res core.Restore) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.db.Restore(res.Snapshot())
	return nil
}
