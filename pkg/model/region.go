package model

import (
	"context"
	"atoms.co/lib-go/pkg/clock"
	"go.atoms.co/splitter/lib/service/location"
	"go.atoms.co/lib/log"
	"go.atoms.co/lib/backoffx"
	"go.atoms.co/lib/randx"
	"sync"
	"time"
)

// Region is a cloud region.
type Region = location.Region

// RegionProvider associates a region for every key. Optimized for lookup. Thread-safe.
type RegionProvider interface {
	Find(key Key) Region
}

func NewRegionProvider(dist Distribution) RegionProvider {
	return &regionProvider{
		initial: dist.Initial(),
		splits:  dist.Splits(),
	}
}

type regionProvider struct {
	initial Region
	splits  []DistributionSplit
}

func (r *regionProvider) Find(key Key) Region {
	for i := len(r.splits); i > 0; i-- {
		if split := r.splits[i-1]; key == split.Key || split.Key.Less(key) {
			return split.Region
		}
	}
	return r.initial
}

type UpdatePlacementFn func() (PlacementInfo, error)

type liveProvider struct {
	cl    clock.Clock
	fn    UpdatePlacementFn
	value PlacementInfo

	provider RegionProvider
	mu       sync.RWMutex
}

// NewLiveRegionProvider returns a periodically updated region provider with the given initial value.
func NewLiveRegionProvider(ctx context.Context, cl clock.Clock, initial PlacementInfo, fn UpdatePlacementFn) RegionProvider {
	ret := &liveProvider{
		cl:       cl,
		fn:       fn,
		value:    initial,
		provider: NewRegionProvider(initial.Placement().Current()),
	}
	go ret.process(ctx)

	log.Infof(ctx, "Created placement watch for %v:v%v, initial=%v", initial.Placement().Name(), initial.Version(), initial.Placement().Current())
	return ret
}

func (p *liveProvider) Find(key Key) Region {
	p.mu.RLock()
	provider := p.provider
	p.mu.RUnlock()

	return provider.Find(key)
}

func (p *liveProvider) process(ctx context.Context) {
	ticker := p.cl.NewTicker(4*time.Minute + randx.Duration(time.Minute))
	defer ticker.Stop()

	updated := p.cl.Now()
	for {
		select {
		case <-ticker.C:
			b := backoffx.NewLimited(10*time.Second, backoffx.WithClock(p.cl), backoffx.WithMaxRetries(3))

			upd, err := backoffx.Retry1(b, p.fn)
			if err != nil {
				log.Warnf(ctx, "Failed to refresh placement %v:v%v, staleness=%v: %v", p.value.Placement().Name(), p.value.Version(), p.cl.Since(updated), err)
				break
			}
			updated = p.cl.Now()

			if upd.Version() <= p.value.Version() {
				break // no change
			}

			log.Infof(ctx, "Updated placement %v:v%v->v%v, updated=%v", p.value.Placement().Name(), p.value.Version(), upd.Version(), upd.Placement().Current())

			provider := NewRegionProvider(upd.Placement().Current())
			p.value = upd

			p.mu.Lock()
			p.provider = provider
			p.mu.Unlock()

		case <-ctx.Done():
			log.Infof(ctx, "Halted placement watch for %v", p.value.Placement().Name())
			return
		}
	}
}
