package cmd

import (
	"context"
	"go.atoms.co/splitter/pkg/core"
	"go.atoms.co/splitter/pkg/model"
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

var (
	coordinatorCommand = &cobra.Command{
		Use:          "coordinator",
		Short:        "Coordinator operation",
		Args:         cobra.ExactArgs(1),
		SilenceUsage: true,
	}
)

func makeCoordinatorInfoCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "info <tenant>/<service>",
		Short:        "Show coordinator information",
		Args:         cobra.ExactArgs(1),
		SilenceUsage: true,
	}

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		name, ok := model.ParseQualifiedServiceNameStr(args[0])
		if !ok {
			return fmt.Errorf("invalid qualified service name: %v", args[0])
		}

		return withInternalClient(func(ctx context.Context, client core.Client) error {
			consumers, snapshot, err := client.CoordinatorInfo(ctx, name)
			if err != nil {
				return err
			}
			for _, info := range consumers {
				printJson(model.UnwrapInstance(info), false)
			}
			printJson(model.UnwrapClusterSnapshot(snapshot), false)
			return nil
		})
	}

	return cmd
}

func makeCoordinatorRestartCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "restart <tenant>/<service>",
		Short:        "Restart coordinator",
		Args:         cobra.ExactArgs(1),
		SilenceUsage: true,
	}

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		name, ok := model.ParseQualifiedServiceNameStr(args[0])
		if !ok {
			return fmt.Errorf("invalid qualified service name: %v", args[0])
		}

		return withInternalClient(func(ctx context.Context, client core.Client) error {
			if err := client.CoordinatorRestart(ctx, name); err != nil {
				return err
			}
			return nil
		})
	}

	return cmd
}

var (
	consumerCommand = &cobra.Command{
		Use:          "consumer",
		Short:        "Coordinator consumer operation",
		Args:         cobra.ExactArgs(1),
		SilenceUsage: true,
	}
)

func makeConsumerSuspendCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "suspend <tenant>/<service> <consumer_id>",
		Short:        "Suspend consumer",
		Args:         cobra.ExactArgs(2),
		SilenceUsage: true,
	}

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		name, ok := model.ParseQualifiedServiceNameStr(args[0])
		if !ok {
			return fmt.Errorf("invalid qualified service name: %v", args[0])
		}

		id := model.InstanceID(args[1])

		return withInternalClient(func(ctx context.Context, client core.Client) error {
			return client.ConsumerSuspend(ctx, name, id)
		})
	}

	return cmd
}

func makeConsumerResumeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "resume <tenant>/<service> <consumer_id>",
		Short:        "Resume consumer",
		Args:         cobra.ExactArgs(2),
		SilenceUsage: true,
	}

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		name, ok := model.ParseQualifiedServiceNameStr(args[0])
		if !ok {
			return fmt.Errorf("invalid qualified service name: %v", args[0])
		}

		id := model.InstanceID(args[1])

		return withInternalClient(func(ctx context.Context, client core.Client) error {
			if err := client.ConsumerResume(ctx, name, id); err != nil {
				return err
			}
			return nil
		})
	}

	return cmd
}

func makeConsumerDrainCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "drain <tenant>/<service> <consumer_id>",
		Short:        "Drain consumer",
		Args:         cobra.ExactArgs(2),
		SilenceUsage: true,
	}

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		name, ok := model.ParseQualifiedServiceNameStr(args[0])
		if !ok {
			return fmt.Errorf("invalid qualified service name: %v", args[0])
		}

		id := model.InstanceID(args[1])

		return withInternalClient(func(ctx context.Context, client core.Client) error {
			if err := client.ConsumerDrain(ctx, name, id); err != nil {
				return err
			}
			return nil
		})
	}

	return cmd
}

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
