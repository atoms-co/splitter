package cmd

import (
	"context"
	"go.atoms.co/slicex"
	"go.atoms.co/splitter/pkg/model"
	"fmt"
	"github.com/spf13/cobra"
)

var (
	tenantCmd = &cobra.Command{
		Use:   "tenants",
		Short: "Manage tenants",
	}
)

func makeListTenantCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "list",
		Short:        "List tenants",
		Args:         cobra.NoArgs,
		SilenceUsage: true,
	}

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		return withClient(func(ctx context.Context, client model.Client) error {
			list, err := client.ListTenants(ctx)
			if err != nil {
				return err
			}
			for _, info := range list {
				printJson(model.UnwrapTenantInfo(info), false)
			}
			return nil
		})
	}

	return cmd
}

func makeNewTenantCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "new <tenant>",
		Short:        "New tenant",
		Args:         cobra.ExactArgs(1),
		SilenceUsage: true,
	}

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		name := model.TenantName(args[0])

		return withClient(func(ctx context.Context, client model.Client) error {
			tenant, err := client.NewTenant(ctx, name, model.NewTenantConfig())
			if err != nil {
				return err
			}
			printJson(model.UnwrapTenantInfo(tenant), true)
			return nil
		})
	}

	return cmd
}

func makeInfoTenantCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "info <tenant>",
		Short:        "Show tenant information",
		Args:         cobra.ExactArgs(1),
		SilenceUsage: true,
	}

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		name := model.TenantName(args[0])

		return withClient(func(ctx context.Context, client model.Client) error {
			info, err := client.InfoTenant(ctx, name)
			if err != nil {
				return err
			}
			printJson(model.UnwrapTenantInfo(info), true)
			return nil
		})
	}

	return cmd
}

func makeUpdateTenantCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "update <tenant>",
		Short:        "Update tenant",
		Args:         cobra.ExactArgs(1),
		SilenceUsage: true,
	}

	banned := cmd.Flags().StringSlice("banned-regions", []string{}, "banned regions")

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		name := model.TenantName(args[0])

		var opts []model.TenantOperationalOption
		if len(*banned) > 0 {
			opts = append(opts, model.WithTenantOperationalBannedRegions(slicex.Map(*banned, func(r string) model.Region {
				return model.Region(r)
			})...))
		}

		if len(opts) == 0 {
			return nil // Nothing to update
		}

		return withClient(func(ctx context.Context, client model.Client) error {
			info, err := client.InfoTenant(ctx, name)
			if err != nil {
				return err
			}

			op, err := model.UpdateTenantOperational(info.Tenant(), opts...)
			if err != nil {
				return err
			}

			updateOpts := []model.UpdateTenantOption{model.WithUpdateTenantOperational(op)}
			info, err = client.UpdateTenant(ctx, info.Name(), info.Version(), updateOpts...)
			if err != nil {
				return err
			}

			printJson(model.UnwrapTenantInfo(info), true)
			return nil
		})
	}

	return cmd
}
func makeDeleteTenantCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "delete <tenant>",
		Short:        "Delete tenant",
		Args:         cobra.ExactArgs(1),
		SilenceUsage: true,
	}

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		name := model.TenantName(args[0])

		return withClient(func(ctx context.Context, client model.Client) error {
			err := client.DeleteTenant(ctx, name)
			if err != nil {
				return fmt.Errorf("delete tenant failed: %v", err)
			}
			return nil
		})
	}

	return cmd
}
