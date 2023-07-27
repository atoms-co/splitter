package memdb

import (
	"context"
	"sync"

	"atoms.co/lib-go/pkg/clock"
	"go.atoms.co/lib/mapx"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pkg/storage"
)

var (
	_ storage.Tenants = (*tenants)(nil)
)

// Storage is an in-memory management storage and provider. Intended for single-node installations with no need for
// persistence, notably tests. Thread-safe.
type Storage struct {
	cl clock.Clock

	tenants map[model.TenantName]model.TenantInfo
	mu      sync.Mutex
}

func New(cl clock.Clock) (*Storage, storage.Management) {
	d := &Storage{
		cl:      cl,
		tenants: map[model.TenantName]model.TenantInfo{},
	}
	return d, storage.Management{
		Tenants: (*tenants)(d),
	}
}

type tenants Storage

func (t *tenants) List(ctx context.Context) ([]model.TenantInfo, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	return mapx.Values(t.tenants), nil
}

func (t *tenants) New(ctx context.Context, tenant model.Tenant) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if _, ok := t.tenants[tenant.Name()]; ok {
		return model.ErrAlreadyExists
	}

	t.tenants[tenant.Name()] = model.NewTenantInfo(tenant, 1, t.cl.Now())
	return nil
}

func (t *tenants) Read(ctx context.Context, name model.TenantName) (model.TenantInfo, error) {
	tenant, ok := t.tenants[name]
	if !ok {
		return model.TenantInfo{}, model.ErrNotFound
	}

	return tenant, nil
}

func (t *tenants) Update(ctx context.Context, tenant model.Tenant, guard storage.UpdateGuard) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	info, ok := t.tenants[tenant.Name()]
	if !ok {
		return model.ErrNotFound
	}
	if info.Version() != guard.Guard {
		return model.ErrVersionMismatch
	}

	t.tenants[tenant.Name()] = model.NewTenantInfo(info.Tenant(), info.Version()+1, t.cl.Now())

	return nil
}

func (t *tenants) Delete(ctx context.Context, name model.TenantName) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	delete(t.tenants, name)
	return nil
}
