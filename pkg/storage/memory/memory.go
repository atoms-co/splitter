package memory

import (
	"context"
	"go.atoms.co/splitter/pkg/storage"
	"sync"
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

func (s *Storage) Read(ctx context.Context) (storage.Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.db.Snapshot(), nil
}

func (s *Storage) Update(ctx context.Context, update storage.Update) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.db.Update(update, false)
}

func (s *Storage) Delete(ctx context.Context, del storage.Delete) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.db.Delete(del)
}
