package leader

import "go.atoms.co/splitter/pkg/storage"

type Leader struct {
	storage storage.Storage
}

func New(storage storage.Storage) *Leader {
	return &Leader{
		storage: storage,
	}
}
