package cmd

import (
	"context"
	"go.atoms.co/splitter/lib/service/location"
	"go.atoms.co/slicex"
	"go.atoms.co/splitter/pkg/model"
	splitter "go.atoms.co/splitter/pkg/model"
	"fmt"
	"github.com/spf13/cobra"
	"strings"
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
				printJson(model.UnwrapServiceInfoEx(info), false)
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

	overrides := cmd.Flags().StringSlice("locality-overrides", []string{}, "locality overrides. e.g. us-west1:centralus")

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		name, ok := splitter.ParseQualifiedServiceNameStr(args[0])
		if !ok {
			return fmt.Errorf("invalid qualified service name: %v", args[0])
		}
		region := splitter.Region(args[1])

		var cfgOpts []splitter.ServiceConfigOption
		cfgOpts = append(cfgOpts, splitter.WithServiceRegion(region))

		if len(*overrides) > 0 {
			locality := map[location.Region]location.Region{}
			for _, override := range *overrides {
				parts := strings.Split(override, ":")
				if len(parts) != 2 {
					return fmt.Errorf("invalid region override: %v", override)
				}
				locality[location.Region(parts[0])] = location.Region(parts[1])
			}
			cfgOpts = append(cfgOpts, splitter.WithLocalityOverrides(locality))
		}

		return withClient(func(ctx context.Context, client model.Client) error {
			service, err := client.NewService(ctx, name, splitter.NewServiceConfig(cfgOpts...))
			if err != nil {
				return err
			}
			printJson(splitter.UnwrapServiceInfo(service), true)
			return nil
		})
	}

	return cmd
}

func makeInfoServiceCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "info <tenant>/<service>",
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
	overrides := cmd.Flags().StringSlice("locality-overrides", []string{}, "locality overrides. e.g. us-west1:centralus")
	banned := cmd.Flags().StringSlice("banned-regions", []string{}, "banned regions")
	disableLB := cmd.Flags().Bool("disable-load-balance", true, "disable load balance")

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		name, ok := splitter.ParseQualifiedServiceNameStr(args[0])
		if !ok {
			return fmt.Errorf("invalid qualified service name: %v", args[0])
		}

		var cfgOpts []splitter.ServiceConfigOption
		if *region != "" {
			cfgOpts = append(cfgOpts, splitter.WithServiceRegion(splitter.Region(*region)))
		}
		if len(*overrides) > 0 {
			locality := map[location.Region]location.Region{}
			for _, override := range *overrides {
				parts := strings.Split(override, ":")
				if len(parts) != 2 {
					return fmt.Errorf("invalid region override: %v", override)
				}
				locality[location.Region(parts[0])] = location.Region(parts[1])
			}
			cfgOpts = append(cfgOpts, splitter.WithLocalityOverrides(locality))
		}

		var opOpts []splitter.ServiceOperationalOption
		if len(*banned) > 0 {
			opOpts = append(opOpts, splitter.WithServiceOperationalBannedRegions(slicex.Map(*banned, func(r string) splitter.Region {
				return splitter.Region(r)
			})...))
		}
		if cmd.Flag("disable-load-balance").Changed {
			opOpts = append(opOpts, splitter.WithServiceOperationalDisableLoadBalance(*disableLB))
		}

		if len(cfgOpts) == 0 && len(opOpts) == 0 {
			return nil // Nothing to update
		}

		return withClient(func(ctx context.Context, client model.Client) error {
			ex, err := client.InfoService(ctx, name)
			if err != nil {
				return err
			}

			// Updated config
			cfg, err := splitter.UpdateServiceConfig(ex.Info().Service(), cfgOpts...)
			if err != nil {
				return err
			}
			// Updated operational metadata
			op, err := splitter.UpdateServiceOperational(ex.Info().Service(), opOpts...)
			if err != nil {
				return err
			}

			updateOpts := []splitter.UpdateServiceOption{splitter.WithUpdateServiceConfig(cfg), splitter.WithUpdateServiceOperational(op)}
			info, err := client.UpdateService(ctx, ex.Name(), ex.Info().Version(), updateOpts...)
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
