package cmd

import (
	"context"
	"go.atoms.co/splitter/pkg/core"
	"go.atoms.co/splitter/pkg/model"
	splitter "go.atoms.co/splitter/pkg/model"
	"fmt"
	"github.com/spf13/cobra"
)

var (
	placementCmd = &cobra.Command{
		Use:   "placement",
		Short: "Manage dynamic domain placements",
	}
)

func makeListPlacementCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "list <tenant>",
		Short:        "List placements for tenant",
		Args:         cobra.ExactArgs(1),
		SilenceUsage: true,
	}

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		tenant := splitter.TenantName(args[0])

		return withInternalClient(func(ctx context.Context, client core.Client) error {
			list, err := client.ListPlacements(ctx, tenant)
			if err != nil {
				return err
			}
			for _, info := range list {
				printJson(core.UnwrapInternalPlacementInfo(info), false)
			}
			return nil
		})
	}

	return cmd
}

func makeNewPlacementCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "new <tenant>/<placement> <region> [block list]",
		Short:        "New placement",
		Args:         cobra.MinimumNArgs(2),
		SilenceUsage: true,
	}

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		name, ok := splitter.ParseQualifiedPlacementNameStr(args[0])
		if !ok {
			return fmt.Errorf("invalid placement name")
		}
		initial := splitter.Region(args[1])

		return withInternalClient(func(ctx context.Context, client core.Client) error {
			info, err := client.NewPlacement(ctx, name, core.NewSingleRegionBlockDistribution(initial))
			if err != nil {
				return fmt.Errorf("placement creation failed: %v", err)
			}
			printJson(core.UnwrapInternalPlacementInfo(info), true)
			return nil
		})
	}

	return cmd
}

func makeInfoPlacementCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "info <tenant>/<placement>",
		Short:        "Show placement information",
		Args:         cobra.ExactArgs(1),
		SilenceUsage: true,
	}

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		name, ok := splitter.ParseQualifiedPlacementNameStr(args[0])
		if !ok {
			return fmt.Errorf("invalid placement name")
		}

		return withInternalClient(func(ctx context.Context, client core.Client) error {
			info, err := client.InfoPlacement(ctx, name)
			if err != nil {
				return fmt.Errorf("info placement failed: %v", err)
			}
			printJson(core.UnwrapInternalPlacementInfo(info), true)
			return nil
		})
	}

	return cmd
}

func makeUpdatePlacementCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "update <tenant>/<placement>",
		Short:        "Update placement",
		Args:         cobra.MinimumNArgs(1),
		SilenceUsage: true,
	}

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		name, ok := splitter.ParseQualifiedPlacementNameStr(args[0])
		if !ok {
			return fmt.Errorf("invalid placement name")
		}

		return withInternalClient(func(ctx context.Context, client core.Client) error {
			info, err := client.InfoPlacement(ctx, name)
			if err != nil {
				return fmt.Errorf("failed to read: %v", err)
			}

			// apply flags ...

			upd, err := client.UpdatePlacement(ctx, name, info.Version())
			if err != nil {
				return fmt.Errorf("update placement failed: %v", err)
			}
			printJson(core.UnwrapInternalPlacementInfo(upd), true)
			return nil
		})
	}

	return cmd
}

func makeDeletePlacementCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "delete <tenant>/<placement>",
		Short:        "Delete placement",
		Args:         cobra.ExactArgs(1),
		SilenceUsage: true,
	}

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		name, ok := splitter.ParseQualifiedPlacementNameStr(args[0])
		if !ok {
			return fmt.Errorf("invalid placement name")
		}

		return withInternalClient(func(ctx context.Context, client core.Client) error {
			err := client.DeletePlacement(ctx, name)
			if err != nil {
				return fmt.Errorf("delete placement failed: %v", err)
			}
			return nil
		})
	}

	return cmd
}

func makePublicInfoPlacementCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "pubinfo <tenant>/<placement>",
		Short:        "Show public placement information",
		Args:         cobra.ExactArgs(1),
		SilenceUsage: true,
	}

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		name, ok := splitter.ParseQualifiedPlacementNameStr(args[0])
		if !ok {
			return fmt.Errorf("invalid placement name")
		}

		return withClient(func(ctx context.Context, client model.Client) error {
			info, err := client.InfoPlacement(ctx, name)
			if err != nil {
				return fmt.Errorf("info placement failed: %v", err)
			}
			printJson(model.UnwrapPlacementInfo(info), true)
			return nil
		})
	}

	return cmd
}
