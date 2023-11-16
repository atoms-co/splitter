package cmd

import (
	"context"
	"go.atoms.co/splitter/pkg/model"
	splitter "go.atoms.co/splitter/pkg/model"
	"fmt"
	"github.com/spf13/cobra"
)

var (
	domainCmd = &cobra.Command{
		Use:   "domains",
		Short: "Manage domains",
	}
)

func makeListDomainCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "list <tenant>/<service>",
		Short:        "List domains",
		Args:         cobra.ExactArgs(1),
		SilenceUsage: true,
	}

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		service, ok := splitter.ParseQualifiedServiceNameStr(args[0])
		if !ok {
			return fmt.Errorf("invalid qualified service name: %v", args[0])
		}
		return withClient(func(ctx context.Context, client model.Client) error {
			list, err := client.ListDomains(ctx, service)
			if err != nil {
				return err
			}
			for _, info := range list {
				printJson(model.UnwrapDomain(info), false)
			}
			return nil
		})
	}

	return cmd
}

var (
	newDomainCmd = &cobra.Command{
		Use:          "new",
		Short:        "New domain",
		Args:         cobra.ExactArgs(1),
		SilenceUsage: true,
	}
)

func makeNewUnitDomainCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "unit <tenant>/<service>/<domain>",
		Short:        "New unit domain",
		Args:         cobra.ExactArgs(1),
		SilenceUsage: true,
	}

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		name, ok := splitter.ParseQualifiedDomainNameStr(args[0])
		if !ok {
			return fmt.Errorf("invalid qualified domain name: %v", args[0])
		}

		return withClient(func(ctx context.Context, client model.Client) error {
			domain, err := client.NewDomain(ctx, name, splitter.Unit, splitter.NewDomainConfig())
			if err != nil {
				return err
			}
			printJson(splitter.UnwrapDomain(domain), true)
			return nil
		})
	}

	return cmd
}

func makeNewGlobalDomainCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "global <tenant>/<service>/<domain>",
		Short:        "New global domain",
		Args:         cobra.ExactArgs(1),
		SilenceUsage: true,
	}

	placement := cmd.Flags().String("placement", "", "Placement name")
	shards := cmd.Flags().Int("shards", 4, "Target shards")

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		name, ok := splitter.ParseQualifiedDomainNameStr(args[0])
		if !ok {
			return fmt.Errorf("invalid qualified domain name: %v", args[0])
		}

		return withClient(func(ctx context.Context, client model.Client) error {
			domain, err := client.NewDomain(ctx, name, splitter.Global,
				splitter.NewDomainConfig(
					splitter.WithDomainPlacement(*placement),
					splitter.WithDomainShardingPolicy(splitter.NewShardingPolicy(*shards)),
				),
			)
			if err != nil {
				return err
			}
			printJson(splitter.UnwrapDomain(domain), true)
			return nil
		})
	}

	return cmd
}

func makeNewRegionalDomainCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "regional <tenant>/<service>/<domain>",
		Short:        "New regional domain",
		Args:         cobra.ExactArgs(1),
		SilenceUsage: true,
	}

	placement := cmd.Flags().String("placement", "", "Placement name")
	shards := cmd.Flags().Int("shards", 4, "Target shards")
	regions := cmd.Flags().StringSlice("regions", []string{"centralus"}, "Regions")

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		name, ok := splitter.ParseQualifiedDomainNameStr(args[0])
		if !ok {
			return fmt.Errorf("invalid qualified domain name: %v", args[0])
		}

		return withClient(func(ctx context.Context, client model.Client) error {
			domain, err := client.NewDomain(ctx, name, splitter.Regional,
				splitter.NewDomainConfig(
					splitter.WithDomainPlacement(*placement),
					splitter.WithDomainShardingPolicy(splitter.NewShardingPolicy(*shards)),
					splitter.WithDomainRegions(*regions...),
				),
			)
			if err != nil {
				return err
			}
			printJson(splitter.UnwrapDomain(domain), true)
			return nil
		})
	}

	return cmd
}

func makeUpdateDomainCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "update <tenant>/<service>/<domain>",
		Short:        "Update domain",
		Args:         cobra.ExactArgs(1),
		SilenceUsage: true,
	}

	state := cmd.Flags().String("state", "", "State")

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		name, ok := splitter.ParseQualifiedDomainNameStr(args[0])
		if !ok {
			return fmt.Errorf("invalid qualified domain name: %v", args[0])
		}

		var opts []splitter.DomainOption
		if state != nil && *state != "" {
			s, ok := splitter.ParseDomainState(*state)
			if !ok {
				return fmt.Errorf("invalid state: %v", *state)
			}
			opts = append(opts, splitter.WithDomainState(s))
		}

		return withClient(func(ctx context.Context, client model.Client) error {
			service, err := client.InfoService(ctx, name.Service)
			if err != nil {
				return err
			}
			domain, ok := service.Domain(name.Domain)
			if !ok {
				return fmt.Errorf("subscription %v not found", name)
			}
			upd, err := splitter.UpdateDomain(domain, opts...)
			if err != nil {
				return err
			}
			t, err := client.UpdateDomain(ctx, upd, service.Info().Version())
			if err != nil {
				return fmt.Errorf("update domain failed: %v", err)
			}
			printJson(splitter.UnwrapDomain(t), true)
			return nil
		})
	}

	return cmd
}
func makeDeleteDomainCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "delete <tenant>/<service>/<domain>",
		Short:        "Delete domain",
		Args:         cobra.ExactArgs(1),
		SilenceUsage: true,
	}

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		name, ok := splitter.ParseQualifiedDomainNameStr(args[0])
		if !ok {
			return fmt.Errorf("invalid qualified domain name: %v", args[0])
		}

		return withClient(func(ctx context.Context, client model.Client) error {
			err := client.DeleteDomain(ctx, name)
			if err != nil {
				return fmt.Errorf("delete domain failed: %v", err)
			}
			return nil
		})
	}

	return cmd
}
