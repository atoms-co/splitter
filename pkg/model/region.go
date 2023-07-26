package model

// Region is a cloud region.
type Region string

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
