package model

import (
	"testing"

	"go.atoms.co/lib/testing/requirex"
)

func TestSplitShards(t *testing.T) {
	tests := []struct {
		name           string
		grantsPerShard []int
		want           [][]int
	}{
		{name: "empty", want: [][]int{{}}},
		{name: "below limit", grantsPerShard: []int{20, 20}, want: [][]int{{20, 20}}},
		{name: "at limit", grantsPerShard: []int{32, 32}, want: [][]int{{32, 32}}},
		{name: "starts new part before exceeding limit", grantsPerShard: []int{63, 2, 1}, want: [][]int{{63}, {2, 1}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			shards := make([]shardLogSnapshot, len(tt.grantsPerShard))
			for i, grantCount := range tt.grantsPerShard {
				shards[i].Grants = make([]grantLogSnapshot, grantCount)
			}

			parts := splitShards(shards)
			got := make([][]int, len(parts))
			for i, part := range parts {
				got[i] = make([]int, len(part))
				for j, shard := range part {
					got[i][j] = len(shard.Grants)
				}
			}
			requirex.Equal(t, got, tt.want)
		})
	}
}
