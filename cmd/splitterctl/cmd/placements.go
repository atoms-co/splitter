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
		Use:   "placements",
		Short: "Manage dynamic domain placements",
	}

	// list of valid regions for placements to prevent typos

	regions = map[model.Region]bool{
		"centralus":      true,
		"northcentralus": true,
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
		Use:          "new <tenant>/<placement> <region>[/block:region]*",
		Short:        "New placement",
		Args:         cobra.ExactArgs(2),
		SilenceUsage: true,
	}

	skip := cmd.Flags().Bool("skip_region_check", false, "Allow use of unsupported regions")

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		name, ok := splitter.ParseQualifiedPlacementNameStr(args[0])
		if !ok {
			return fmt.Errorf("invalid placement name")
		}
		dist, err := core.ParseBlockDistributionStr(args[1])
		if err != nil {
			return fmt.Errorf("invalid distribution: %v", err)
		}

		if !*skip {
			if err := validateBlockDistribution(dist); err != nil {
				return err
			}
		}

		return withInternalClient(func(ctx context.Context, client core.Client) error {
			info, err := client.NewPlacement(ctx, name, dist)
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
		Args:         cobra.ExactArgs(1),
		SilenceUsage: true,
	}

	state := cmd.Flags().String("state", "", "Update state")
	target := cmd.Flags().String("target", "", "Update target distribution as <region>[/block:region]*")
	blocks := cmd.Flags().Int("speed", 0, "Blocks to move per cycle (optional)")

	skip := cmd.Flags().Bool("skip_region_check", false, "Allow use of unsupported regions")
	force := cmd.Flags().Bool("force", false, "Force immediate application")

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

			var opts []core.UpdatePlacementOption

			if *state != "" {
				upd, ok := core.ParsePlacementState(*state)
				if !ok {
					return fmt.Errorf("invalid placement state: %v", *state)
				}
				opts = append(opts, core.WithPlacementState(upd))
			}

			if *target != "" {
				dist, err := core.ParseBlockDistributionStr(*target)
				if err != nil {
					return fmt.Errorf("invalid target distribution: %v", err)
				}
				if !*skip {
					if err := validateBlockDistribution(dist); err != nil {
						return err
					}
				}

				current := info.InternalPlacement().Current()
				if *force {
					current = dist
				}
				speed := info.InternalPlacement().BlocksPerCycle()
				if *blocks > 0 {
					speed = *blocks
				}
				opts = append(opts, core.WithPlacementConfig(core.NewInternalPlacementConfig(dist, current, speed)))
			}

			upd, err := client.UpdatePlacement(ctx, name, info.Version(), opts...)
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

func validateBlockDistribution(dist core.BlockDistribution) error {
	if !regions[dist.Initial()] {
		return fmt.Errorf("invalid region: %v", dist.Initial())
	}
	for _, s := range dist.Splits() {
		if !regions[s.Region] {
			return fmt.Errorf("invalid region: %v", s)
		}
	}
	return nil
}
