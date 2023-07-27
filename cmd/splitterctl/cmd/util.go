package cmd

import (
	"context"
	"go.atoms.co/lib/net/grpcx"
	splitter "go.atoms.co/splitter/pkg/model"
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

// withClient runs a function in a KEQ2 client context.
func withClient(fn func(ctx context.Context, client splitter.Client) error) error {
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

	return fn(ctx, splitter.NewClient(cc))
}

func printJson(pb proto.Message, multiLine bool) {
	buf, _ := protojson.MarshalOptions{Multiline: multiLine}.Marshal(pb)
	fmt.Println(string(buf))
}
