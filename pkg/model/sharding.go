package model

import "go.atoms.co/splitter/pb"

// ShardingPolicy represents a configurable shard policy for Splitter
type ShardingPolicy struct {
	pb *public_v1.ShardingPolicy
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
