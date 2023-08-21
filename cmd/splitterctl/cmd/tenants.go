package cmd

import (
	"context"
	"go.atoms.co/splitter/pkg/model"
	splitter "go.atoms.co/splitter/pkg/model"
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
		Use:          "new <tenant> <region>",
		Short:        "New tenant",
		Args:         cobra.ExactArgs(2),
		SilenceUsage: true,
	}

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		name := splitter.TenantName(args[0])
		region := splitter.Region(args[1])

		return withClient(func(ctx context.Context, client model.Client) error {
			tenant, err := client.NewTenant(ctx, name, splitter.NewTenantConfig(region))
			if err != nil {
				return err
			}
			printJson(splitter.UnwrapTenant(tenant), true)
			return nil
		})
	}

	return cmd
}

func makeReadTenantCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "read <tenant>",
		Short:        "Show tenant information",
		Args:         cobra.ExactArgs(1),
		SilenceUsage: true,
	}

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		name := splitter.TenantName(args[0])

		return withClient(func(ctx context.Context, client model.Client) error {
			info, err := client.ReadTenant(ctx, name)
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

	region := cmd.Flags().String("region", "", "Region")

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		name := splitter.TenantName(args[0])

		return withClient(func(ctx context.Context, client model.Client) error {
			info, err := client.ReadTenant(ctx, name)
			if err != nil {
				return err
			}

			var opts []splitter.TenantConfigOption
			if *region != "" {
				opts = append(opts, splitter.WithTenantRegion(splitter.Region(*region)))
			}
			cfg, err := splitter.UpdateTenantConfig(info.Tenant(), opts...)
			if err != nil {
				return err
			}

			updateOpts := []splitter.UpdateTenantOption{splitter.WithUpdateTenantConfig(cfg)}
			info, err = client.UpdateTenant(ctx, info, updateOpts...)
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
		name := splitter.TenantName(args[0])

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
