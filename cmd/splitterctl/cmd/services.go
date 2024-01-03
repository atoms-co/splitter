package cmd

import (
	"context"
	"go.atoms.co/splitter/pkg/model"
	splitter "go.atoms.co/splitter/pkg/model"
	"fmt"
	"github.com/spf13/cobra"
)

var (
	serviceCmd = &cobra.Command{
		Use:   "services",
		Short: "Manage services",
	}
)

func makeListServiceCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "list <tenant>",
		Short:        "List services",
		Args:         cobra.ExactArgs(1),
		SilenceUsage: true,
	}

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		tenant := splitter.TenantName(args[0])

		return withClient(func(ctx context.Context, client model.Client) error {
			list, err := client.ListServices(ctx, tenant)
			if err != nil {
				return err
			}
			for _, info := range list {
				printJson(model.UnwrapServiceInfoEx(info), true)
			}
			return nil
		})
	}

	return cmd
}

func makeNewServiceCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "new <tenant>/<service> <region>",
		Short:        "New service",
		Args:         cobra.ExactArgs(2),
		SilenceUsage: true,
	}

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		name, ok := splitter.ParseQualifiedServiceNameStr(args[0])
		if !ok {
			return fmt.Errorf("invalid qualified service name: %v", args[0])
		}
		region := splitter.Region(args[1])

		return withClient(func(ctx context.Context, client model.Client) error {
			service, err := client.NewService(ctx, name, splitter.NewServiceConfig(splitter.WithServiceRegion(region)))
			if err != nil {
				return err
			}
			printJson(splitter.UnwrapServiceInfo(service), true)
			return nil
		})
	}

	return cmd
}

func makeReadServiceCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "read <tenant>/<service>",
		Short:        "Show service information",
		Args:         cobra.ExactArgs(1),
		SilenceUsage: true,
	}

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		name, ok := splitter.ParseQualifiedServiceNameStr(args[0])
		if !ok {
			return fmt.Errorf("invalid qualified service name: %v", args[0])
		}

		return withClient(func(ctx context.Context, client model.Client) error {
			ex, err := client.InfoService(ctx, name)
			if err != nil {
				return err
			}
			printJson(model.UnwrapServiceInfoEx(ex), true)
			return nil
		})
	}

	return cmd
}

func makeUpdateServiceCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "update <tenant>/<service>",
		Short:        "Update service",
		Args:         cobra.ExactArgs(1),
		SilenceUsage: true,
	}

	region := cmd.Flags().String("region", "", "Region")

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		name, ok := splitter.ParseQualifiedServiceNameStr(args[0])
		if !ok {
			return fmt.Errorf("invalid qualified service name: %v", args[0])
		}

		return withClient(func(ctx context.Context, client model.Client) error {
			ex, err := client.InfoService(ctx, name)
			if err != nil {
				return err
			}

			var opts []splitter.ServiceConfigOption
			if *region != "" {
				opts = append(opts, splitter.WithServiceRegion(splitter.Region(*region)))
			}
			cfg, err := splitter.UpdateServiceConfig(ex.Info().Service(), opts...)
			if err != nil {
				return err
			}

			updateOpts := []splitter.UpdateServiceOption{splitter.WithUpdateServiceConfig(cfg)}
			info, err := client.UpdateService(ctx, ex.Info(), updateOpts...)
			if err != nil {
				return err
			}
			printJson(model.UnwrapServiceInfo(info), true)
			return nil
		})
	}

	return cmd
}
func makeDeleteServiceCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "delete <tenant>/<service>",
		Short:        "Delete service",
		Args:         cobra.ExactArgs(1),
		SilenceUsage: true,
	}

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		name, ok := splitter.ParseQualifiedServiceNameStr(args[0])
		if !ok {
			return fmt.Errorf("invalid qualified service name: %v", args[0])
		}

		return withClient(func(ctx context.Context, client model.Client) error {
			err := client.DeleteService(ctx, name)
			if err != nil {
				return fmt.Errorf("delete service failed: %v", err)
			}
			return nil
		})
	}

	return cmd
}
