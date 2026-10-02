package model

import (
	"context"
	"runtime"
	"sync"
	"testing"
	"time"

	"go.atoms.co/lib/log"
	"go.atoms.co/lib/testing/requirex"
)

type grantLogCall struct {
	depth int
	file  string
	line  int
}

type grantRecordingLogger struct {
	mu    sync.Mutex
	calls []grantLogCall
}

func (l *grantRecordingLogger) Log(_ context.Context, _ log.Severity, calldepth int, _ string) {
	// Like the production backends, skip the logger's own frame as well.
	_, file, line, _ := runtime.Caller(calldepth + 1)
	l.mu.Lock()
	defer l.mu.Unlock()
	l.calls = append(l.calls, grantLogCall{depth: calldepth, file: file, line: line})
}

func (l *grantRecordingLogger) Flush(context.Context) error { return nil }

func recordGrantLogs(t *testing.T) *grantRecordingLogger {
	t.Helper()
	// These tests must remain sequential because the logging backend is global.
	l := &grantRecordingLogger{}
	log.SetLogger(l)
	t.Cleanup(func() {
		log.SetLogger(&log.Standard{})
	})
	return l
}

func TestLogGrantsConsumerCaller(t *testing.T) {
	l := recordGrantLogs(t)
	shards := []shardLogSnapshot{
		{Grants: make([]grantLogSnapshot, maxGrantsPerLog)},
		{Grants: make([]grantLogSnapshot, 1)},
	}

	_, file, line, _ := runtime.Caller(0)
	logGrants(context.Background(), log.SevInfo, "Consumer grant states", grantLogSourceConsumer, time.Time{}, shards, 0)

	l.mu.Lock()
	defer l.mu.Unlock()
	requirex.Equal(t, l.calls, []grantLogCall{
		{depth: 2, file: file, line: line + 1},
		{depth: 2, file: file, line: line + 1},
	})
}

func TestLogCoordinatorGrantsCaller(t *testing.T) {
	l := recordGrantLogs(t)
	cluster := NewClusterMap(ClusterID{}, nil)

	_, file, line, _ := runtime.Caller(0)
	LogCoordinatorGrants(context.Background(), log.SevInfo, "Coordinator grant states", time.Time{}, cluster)

	l.mu.Lock()
	defer l.mu.Unlock()
	requirex.Equal(t, l.calls, []grantLogCall{{depth: 3, file: file, line: line + 1}})
}

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
