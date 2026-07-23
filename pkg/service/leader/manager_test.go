package leader_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/stats"

	"go.atoms.co/splitter/pkg/service/leader"
)

func TestManagerClosesRemoteConnectionOnLeadershipChange(t *testing.T) {
	connections := &connectionStats{
		started: make(chan struct{}, 1),
		ended:   make(chan struct{}, 1),
	}
	server := grpc.NewServer(grpc.StatsHandler(connections))
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() {
		_ = server.Serve(listener)
	}()
	defer server.Stop()

	directives := make(chan leader.Directive)
	manager := leader.NewManager(directives, nil)
	defer func() {
		close(directives)
		<-manager.Closed()
	}()

	directives <- leader.Directive{Type: leader.Follow, ID: "leader1", Endpoint: listener.Addr().String()}
	select {
	case <-connections.started:
	case <-time.After(5 * time.Second):
		t.Fatal("manager did not connect to remote leader")
	}

	directives <- leader.Directive{Type: leader.Disconnect}
	select {
	case <-connections.ended:
	case <-time.After(5 * time.Second):
		t.Fatal("manager did not close remote leader connection")
	}
}

type connectionStats struct {
	started chan struct{}
	ended   chan struct{}
}

func (s *connectionStats) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context {
	return ctx
}

func (s *connectionStats) HandleRPC(context.Context, stats.RPCStats) {}

func (s *connectionStats) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}

func (s *connectionStats) HandleConn(_ context.Context, event stats.ConnStats) {
	switch event.(type) {
	case *stats.ConnBegin:
		select {
		case s.started <- struct{}{}:
		default:
		}
	case *stats.ConnEnd:
		select {
		case s.ended <- struct{}{}:
		default:
		}
	}
}
