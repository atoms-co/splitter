package cmd

import (
	"context"
	"go.atoms.co/splitter/pkg/core"
	"fmt"
	"github.com/spf13/cobra"
)

var (
	raftCmd = &cobra.Command{
		Use:   "raft",
		Short: "Manage Tenants",
	}
)

func makeJoinRaftCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "join <id> <addr>",
		Short:        "join raft group",
		Args:         cobra.ExactArgs(2),
		SilenceUsage: true,
	}

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		id := args[0]
		addr := args[1]
		return withInternalClient(func(ctx context.Context, client core.Client) error {
			err := client.RaftJoin(ctx, id, addr)
			if err != nil {
				return fmt.Errorf("raft join failed: %v", err)
			}
			return nil
		})
	}

	return cmd
}
