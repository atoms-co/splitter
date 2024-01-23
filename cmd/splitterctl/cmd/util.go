package cmd

import (
	"context"
	"go.atoms.co/lib/net/grpcx"
	"go.atoms.co/lib/statshandlerx"
	"go.atoms.co/splitter/pkg/core"
	"go.atoms.co/splitter/pkg/model"
	"fmt"
	_ "github.com/Jille/grpc-multi-resolver"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"strings"
	"time"
)

var (
	endpoint    string
	endpoints   []string
	dialTimeout time.Duration
	insecure    bool
)

func withClient(fn func(ctx context.Context, client model.Client) error) error {
	ctx := context.Background()

	opts := []grpc.DialOption{
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(1 << 30)), // 1GB
	}
	if insecure {
		opts = append(opts, grpcx.WithInsecure())
	}

	// simulate load-balancing using Jille/grpc-multi-resolver library
	if len(endpoints) > 0 {
		ctx, cancel := context.WithTimeout(ctx, dialTimeout)
		defer cancel()
		cc, err := grpc.DialContext(
			ctx,
			fmt.Sprintf("multi:///%s", strings.Join(endpoints, ",")),
			append(opts, statshandlerx.WithClientGRPCStatsHandler())...,
		)
		if err != nil {
			return err
		}

		defer func() { _ = cc.Close() }()

		return fn(ctx, model.NewClient(cc))
	}

	cc, err := grpcx.Dial(ctx, endpoint, dialTimeout, opts...)
	if err != nil {
		return err
	}

	defer func() { _ = cc.Close() }()

	return fn(ctx, model.NewClient(cc))
}

func withInternalClient(fn func(ctx context.Context, client core.Client) error) error {
	ctx := context.Background()

	opts := []grpc.DialOption{
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(1 << 30)), // 1GB
	}
	if insecure {
		opts = append(opts, grpcx.WithInsecure())
	}

	cc, err := grpcx.Dial(ctx, endpoint, dialTimeout, opts...)
	if err != nil {
		return err
	}
	defer func() { _ = cc.Close() }()

	return fn(ctx, core.NewClient(cc))
}

func printJson(pb proto.Message, multiLine bool) {
	buf, _ := protojson.MarshalOptions{Multiline: multiLine}.Marshal(pb)
	fmt.Println(string(buf))
}
