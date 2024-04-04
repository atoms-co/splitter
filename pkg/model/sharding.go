package model

import (
	"go.atoms.co/lib/mapx"
	"go.atoms.co/slicex"
	"go.atoms.co/splitter/pb"
	"fmt"
	"github.com/golang/protobuf/proto"
	"sort"
	"sync"
)

type ShardingPolicyOption func(policy *public_v1.ShardingPolicy)

func WithShards(shards int) ShardingPolicyOption {
	return func(policy *public_v1.ShardingPolicy) {
		policy.Shards = int64(shards)
	}
}

// ShardingPolicy represents a configurable shard policy for Splitter
type ShardingPolicy struct {
	pb *public_v1.ShardingPolicy
}

func NewShardingPolicy(shards int, opts ...ShardingPolicyOption) ShardingPolicy {
	pb := &public_v1.ShardingPolicy{Shards: int64(shards)}
	for _, fn := range opts {
		fn(pb)
	}
	return ShardingPolicy{pb: pb}
}

func UpdateShardingPolicy(policy ShardingPolicy, opts ...ShardingPolicyOption) ShardingPolicy {
	pb := proto.Clone(UnwrapShardingPolicy(policy)).(*public_v1.ShardingPolicy)
	for _, fn := range opts {
		fn(pb)
	}
	return WrapShardingPolicy(pb)
}

func WrapShardingPolicy(pb *public_v1.ShardingPolicy) ShardingPolicy {
	return ShardingPolicy{pb: pb}
}

func UnwrapShardingPolicy(policy ShardingPolicy) *public_v1.ShardingPolicy {
	return policy.pb
}

func (p ShardingPolicy) Shards() int {
	return int(p.pb.GetShards())
}

// ShardKV is a key-value with a shard.
type ShardKV[K comparable, V any] struct {
	Shard Shard
	K     K
	V     V
}

func (s ShardKV[K, V]) String() string {
	return fmt.Sprintf("%v;%v=%v", s.Shard, s.K, s.V)
}

// ShardMap is map (Shard, K) -> V optimized for domain key lookup without K. Shards must be disjoint or identical.
// Performance is optimized for disjoint shards with a high lookup-to-write ratio. Not thread-safe.
type ShardMap[K comparable, V any] struct {
	m map[QualifiedDomainName]*domainEntry[K, V]
}

func NewShardMap[K comparable, V any]() *ShardMap[K, V] {
	return &ShardMap[K, V]{
		m: map[QualifiedDomainName]*domainEntry[K, V]{},
	}
}

func (s *ShardMap[K, V]) Domains() []QualifiedDomainName {
	return mapx.Keys(s.m)
}

func (s *ShardMap[K, V]) Domain(name QualifiedDomainName) []ShardKV[K, V] {
	if domain, ok := s.m[name]; ok {
		return mapx.MapToSlice(domain.m, func(k shardKey[K], v V) ShardKV[K, V] {
			return ShardKV[K, V]{Shard: k.shard, K: k.key, V: v}
		})
	}
	return nil
}

func (s *ShardMap[K, V]) Read(shard Shard, k K) (V, bool) {
	if domain, ok := s.m[shard.Domain]; ok {
		v, ok := domain.m[shardKey[K]{shard: shard, key: k}]
		return v, ok
	}

	var zero V
	return zero, false
}

func (s *ShardMap[K, V]) Write(shard Shard, k K, v V) {
	domain, ok := s.m[shard.Domain]
	if !ok {
		domain = &domainEntry[K, V]{
			t: shard.Type,
			m: map[shardKey[K]]V{},
		}
		s.m[shard.Domain] = domain
	}

	domain.m[shardKey[K]{shard: shard, key: k}] = v

	domain.mu.Lock()
	defer domain.mu.Unlock()

	domain.invalidate()
}

func (s *ShardMap[K, V]) Delete(shard Shard, k K) {
	domain, ok := s.m[shard.Domain]
	if !ok {
		return
	}

	delete(domain.m, shardKey[K]{shard: shard, key: k})

	domain.mu.Lock()
	defer domain.mu.Unlock()

	domain.invalidate()
}

func (s *ShardMap[K, V]) Lookup(key QualifiedDomainKey) []ShardKV[K, V] {
	domain, ok := s.m[key.Domain]
	if !ok {
		return nil
	}

	domain.mu.Lock()
	defer domain.mu.Unlock()

	domain.initIfNeeded()
	switch domain.t {
	case Unit:
		return slicex.Clone(domain.unit)
	case Global:
		return findEnclosing(key, domain.global)
	case Regional:
		return findEnclosing(key, domain.regional[key.Key.Region])
	default:
		return nil
	}
}

type shardKey[K comparable] struct {
	shard Shard
	key   K
}

type domainEntry[K comparable, V any] struct {
	t DomainType
	m map[shardKey[K]]V

	unit     []ShardKV[K, V]            // if non-nil, all shards
	global   []ShardKV[K, V]            // if non-nil, sorted by start uuid populated lazily.
	regional map[Region][]ShardKV[K, V] // if non-nil, sorted by start uuid populated lazily.
	mu       sync.Mutex
}

func (d *domainEntry[K, V]) initIfNeeded() {
	switch d.t {
	case Unit:
		if d.unit != nil {
			return
		}

		// Prepare data, but leave unsorted. Any lookup returns all shards.

		for k, v := range d.m {
			d.unit = append(d.unit, ShardKV[K, V]{Shard: k.shard, K: k.key, V: v})
		}

	case Global:
		if d.global != nil {
			return
		}

		// Prepare sorted cache for binary-search.

		for k, v := range d.m {
			d.global = append(d.global, ShardKV[K, V]{Shard: k.shard, K: k.key, V: v})
		}
		sort.Slice(d.global, func(i, j int) bool {
			return d.global[i].Shard.From.Less(d.global[j].Shard.From)
		})

	case Regional:
		if d.regional != nil {
			return
		}

		// Prepare sorted cache for binary-search by region.

		d.regional = map[Region][]ShardKV[K, V]{}
		for k, v := range d.m {
			d.regional[k.shard.Region] = append(d.regional[k.shard.Region], ShardKV[K, V]{Shard: k.shard, K: k.key, V: v})
		}
		for _, list := range d.regional {
			sort.Slice(list, func(i, j int) bool {
				return list[i].Shard.From.Less(list[j].Shard.From)
			})
		}

	default:
		// Ignore: unexpected topic type
	}
}

func (d *domainEntry[K, V]) invalidate() {
	d.unit = nil
	d.global = nil
	d.regional = nil
}

// findEnclosing returns all shards that include the key. Assumes sorted list.
func findEnclosing[K comparable, V any](key QualifiedDomainKey, list []ShardKV[K, V]) []ShardKV[K, V] {
	first := sort.Search(len(list), func(i int) bool {
		return key.Key.Key.Less(list[i].Shard.To) // key < to
	})

	// first == index of first candidate

	var ret []ShardKV[K, V]
	for i := first; i < len(list); i++ {
		if elm := list[i]; elm.Shard.Contains(key) {
			ret = append(ret, elm)
		} else {
			break // past window
		}
	}
	return ret
}
