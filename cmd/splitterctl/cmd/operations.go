package cmd

import (
	"context"
	"go.atoms.co/splitter/pkg/core"
	"encoding/json"
	"fmt"
	"github.com/spf13/cobra"
)

var (
	operationCmd = &cobra.Command{
		Use:   "operations",
		Short: "Operations",
	}
)

func makeRaftInfoCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "raft-info",
		Short:        "Show raft information for a node",
		Args:         cobra.NoArgs,
		SilenceUsage: true,
	}

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		return withInternalClient(func(ctx context.Context, client core.Client) error {
			info, err := client.RaftInfo(ctx)
			if err != nil {
				return err
			}
			stats, _ := json.Marshal(info)
			fmt.Println(stats)
			return nil
		})
	}

	return cmd
}

func makeRestoreCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "restore",
		Short:        "Restore the RAFT log with the current leader state",
		Args:         cobra.NoArgs,
		SilenceUsage: true,
	}

	nuke := cmd.Flags().Bool("nuke", false, "Set the raft log to an empty snapshot")

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		return withInternalClient(func(ctx context.Context, client core.Client) error {
			snapshot, err := client.Restore(ctx, *nuke)
			if err != nil {
				return err
			}
			printJson(core.UnwrapSnapshot(snapshot), false)
			return nil
		})
	}

	return cmd
}
