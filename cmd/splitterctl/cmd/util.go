package cmd

import (
	"context"
	"go.atoms.co/lib/net/grpcx"
	"go.atoms.co/splitter/pkg/core"
	"go.atoms.co/splitter/pkg/model"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"time"
)

var (
	endpoint    string
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
