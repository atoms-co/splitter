package cmd

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/spf13/cobra"

	"go.atoms.co/splitter/pkg/core"
)

var (
	raftCommand = &cobra.Command{
		Use:          "raft",
		Short:        "Raft operations",
		SilenceUsage: true,
	}
)

func makeRaftInfoCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "info",
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
			fmt.Println(string(stats))
			return nil
		})
	}

	return cmd
}

func makeRaftAddCmd() *cobra.Command {
	var id, address string

	cmd := &cobra.Command{
		Use:          "add",
		Short:        "Add a node to the RAFT cluster",
		Args:         cobra.NoArgs,
		SilenceUsage: true,
	}

	cmd.Flags().StringVar(&id, "id", "", "raft id of the node to add")
	cmd.Flags().StringVar(&address, "address", "", "raft address of the node to add")
	_ = cmd.MarkFlagRequired("id")
	_ = cmd.MarkFlagRequired("address")

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		return withInternalClient(func(ctx context.Context, client core.Client) error {
			if err := client.RaftAddNode(ctx, id, address); err != nil {
				return err
			}
			fmt.Printf("Added node %v@%v to the RAFT cluster\n", id, address)
			return nil
		})
	}

	return cmd
}

func makeRaftRemoveCmd() *cobra.Command {
	var id string

	cmd := &cobra.Command{
		Use:          "remove",
		Short:        "Remove a node from the RAFT cluster",
		Args:         cobra.NoArgs,
		SilenceUsage: true,
	}

	cmd.Flags().StringVar(&id, "id", "", "raft id of the node to remove")
	_ = cmd.MarkFlagRequired("id")

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		return withInternalClient(func(ctx context.Context, client core.Client) error {
			if err := client.RaftRemoveNode(ctx, id); err != nil {
				return err
			}
			fmt.Printf("Removed node %v from the RAFT cluster\n", id)
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
