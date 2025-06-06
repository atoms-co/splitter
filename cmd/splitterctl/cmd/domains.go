package cmd

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/spf13/cobra"

	"go.atoms.co/slicex"
	"go.atoms.co/lib/stringx"
	"go.atoms.co/splitter/pkg/model"
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
		service, ok := model.ParseQualifiedServiceNameStr(args[0])
		if !ok {
			return fmt.Errorf("invalid qualified service name: %v", args[0])
		}
		return withClient(func(ctx context.Context, client model.Client) error {
			list, err := client.ListDomains(ctx, service)
			if err != nil {
				return err
			}
			sort.Slice(list, func(i, j int) bool {
				return list[i].Name().Domain < list[j].Name().Domain
			})
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
	state := cmd.Flags().String("state", "", "Domain state")

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		name, ok := model.ParseQualifiedDomainNameStr(args[0])
		if !ok {
			return fmt.Errorf("invalid qualified domain name: %v", args[0])
		}

		var opts []model.NewDomainOption
		if *state != "" {
			s, ok := model.ParseDomainState(*state)
			if !ok {
				return fmt.Errorf("invalid state: %v", *state)
			}
			opts = append(opts, model.WithNewDomainState(s))
		}

		return withClient(func(ctx context.Context, client model.Client) error {
			domain, err := client.NewDomain(ctx, name, model.Unit, model.NewDomainConfig(), opts...)
			if err != nil {
				return err
			}
			printJson(model.UnwrapDomain(domain), true)
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
	affinity := cmd.Flags().StringSlice("anti-affinity", []string{}, "Anti affinity domains")
	namedKeys := cmd.Flags().StringSlice("namedKeys", []string{}, "Named domain keys e.g. Ruff:b188ea31-f889-4ce5-9fc9-77fda8ab5c83")
	state := cmd.Flags().String("state", "", "Domain state")

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		name, ok := model.ParseQualifiedDomainNameStr(args[0])
		if !ok {
			return fmt.Errorf("invalid qualified domain name: %v", args[0])
		}

		keys, err := parseNamedDomainKeys(*namedKeys, model.Global, []model.Region{})
		if err != nil {
			return err
		}

		var opts []model.NewDomainOption
		if *state != "" {
			s, ok := model.ParseDomainState(*state)
			if !ok {
				return fmt.Errorf("invalid state: %v", *state)
			}
			opts = append(opts, model.WithNewDomainState(s))
		}

		return withClient(func(ctx context.Context, client model.Client) error {
			domain, err := client.NewDomain(ctx, name, model.Global,
				model.NewDomainConfig(
					model.WithDomainPlacement(model.PlacementName(*placement)),
					model.WithDomainShardingPolicy(model.NewShardingPolicy(*shards)),
					model.WithDomainNamedKeys(keys...),
					model.WithDomainAntiAffinity(slicex.Map(*affinity, func(t string) model.DomainName {
						return model.DomainName(t)
					})...),
				),
				opts...,
			)
			if err != nil {
				return err
			}
			printJson(model.UnwrapDomain(domain), true)
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
	affinity := cmd.Flags().StringSlice("anti-affinity", []string{}, "Anti affinity domains")
	namedKeys := cmd.Flags().StringSlice("namedKeys", []string{}, "Named domain keys e.g. Ruff:centralus:b188ea31-f889-4ce5-9fc9-77fda8ab5c83")
	state := cmd.Flags().String("state", "", "Domain state")

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		name, ok := model.ParseQualifiedDomainNameStr(args[0])
		if !ok {
			return fmt.Errorf("invalid qualified domain name: %v", args[0])
		}

		regions := slicex.Map(*regions, stringx.FromString[model.Region])

		keys, err := parseNamedDomainKeys(*namedKeys, model.Regional, regions)
		if err != nil {
			return err
		}

		var opts []model.NewDomainOption
		if *state != "" {
			s, ok := model.ParseDomainState(*state)
			if !ok {
				return fmt.Errorf("invalid state: %v", *state)
			}
			opts = append(opts, model.WithNewDomainState(s))
		}

		return withClient(func(ctx context.Context, client model.Client) error {
			domain, err := client.NewDomain(ctx, name, model.Regional,
				model.NewDomainConfig(
					model.WithDomainPlacement(model.PlacementName(*placement)),
					model.WithDomainShardingPolicy(model.NewShardingPolicy(*shards)),
					model.WithDomainNamedKeys(keys...),
					model.WithDomainRegions(regions...),
					model.WithDomainAntiAffinity(slicex.Map(*affinity, func(t string) model.DomainName {
						return model.DomainName(t)
					})...),
				),
				opts...,
			)
			if err != nil {
				return err
			}
			printJson(model.UnwrapDomain(domain), true)
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
	banned := cmd.Flags().StringSlice("banned-regions", []string{}, "banned regions")
	locked := cmd.Flags().Bool("locked", false, "Locked operational state")
	shards := cmd.Flags().Int("shards", -1, "Shard count")
	namedKeys := cmd.Flags().StringSlice("namedKeys", []string{}, "Named domain keys e.g. Ruff:centralus:b188ea31-f889-4ce5-9fc9-77fda8ab5c83")

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		name, ok := model.ParseQualifiedDomainNameStr(args[0])
		if !ok {
			return fmt.Errorf("invalid qualified domain name: %v", args[0])
		}

		var opts []model.UpdateDomainOption
		if state != nil && *state != "" {
			s, ok := model.ParseDomainState(*state)
			if !ok {
				return fmt.Errorf("invalid state: %v", *state)
			}
			opts = append(opts, model.WithUpdateDomainState(s))
		}

		var cfgOptions []model.DomainConfigOption

		var sCfgOpts []model.ShardingPolicyOption
		if *shards != -1 {
			sCfgOpts = append(sCfgOpts, model.WithShards(*shards))
		}

		var opOptions []model.DomainOperationalOption
		if cmd.Flags().Changed("banned-regions") {
			opOptions = append(opOptions, model.WithDomainOperationalBannedRegions(slicex.Map(*banned, func(r string) model.Region {
				return model.Region(r)
			})...))
		}
		if cmd.Flag("locked").Changed {
			opOptions = append(opOptions, model.WithDomainOperationalLocked(*locked))
		}

		if len(opts) == 0 && len(opOptions) == 0 && len(sCfgOpts) == 0 {
			return nil // nothing to update
		}

		return withClient(func(ctx context.Context, client model.Client) error {
			service, err := client.InfoService(ctx, name.Service)
			if err != nil {
				return err
			}
			domain, ok := service.Domain(name.Domain)
			if !ok {
				return fmt.Errorf("unknown domain: %v", name)
			}

			if len(*namedKeys) > 0 {
				regions, _ := domain.Regions()

				keys, err := parseNamedDomainKeys(*namedKeys, domain.Type(), regions)
				if err != nil {
					return err
				}

				if len(keys) > 0 {
					cfgOptions = append(cfgOptions, model.WithDomainNamedKeys(keys...))
				}
			}

			if len(opOptions) > 0 {
				op, err := model.UpdateDomainOperational(domain, opOptions...)
				if err != nil {
					return err
				}
				opts = append(opts, model.WithUpdateDomainOperational(op))
			}

			if len(sCfgOpts) > 0 {
				cfgOptions = append(cfgOptions, model.WithDomainShardingPolicy(model.UpdateShardingPolicy(domain.Config().ShardingPolicy(), sCfgOpts...)))
			}

			if len(cfgOptions) > 0 {
				updCfg, err := model.UpdateDomainConfig(domain, cfgOptions...)
				if err != nil {
					return err
				}
				opts = append(opts, model.WithUpdateDomainConfig(updCfg))
			}

			t, err := client.UpdateDomain(ctx, name, service.Info().Version(), opts...)
			if err != nil {
				return fmt.Errorf("update domain failed: %v", err)
			}
			printJson(model.UnwrapDomain(t), true)
			return nil
		})
	}

	return cmd
}

func makeInfoDomainCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "info <tenant>/<service>/<domain>",
		Short:        "Show domain information",
		Args:         cobra.ExactArgs(1),
		SilenceUsage: true,
	}

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		name, ok := model.ParseQualifiedDomainNameStr(args[0])
		if !ok {
			return fmt.Errorf("invalid qualified domain name: %v", args[0])
		}

		return withClient(func(ctx context.Context, client model.Client) error {
			service, err := client.InfoService(ctx, name.Service)
			if err != nil {
				return err
			}

			domain, ok := service.Domain(name.Domain)
			if !ok {
				return fmt.Errorf("unknown domain: %v", name)
			}

			printJson(model.UnwrapDomain(domain), true)
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
		name, ok := model.ParseQualifiedDomainNameStr(args[0])
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

func parseNamedDomainKeys(namedKeyStrs []string, domainType model.DomainType, allowedRegions []model.Region) ([]model.NamedDomainKey, error) {
	var keys []model.NamedDomainKey
	for _, name := range namedKeyStrs {
		parts := strings.Split(name, ":")

		if parts[0] == "" {
			return nil, fmt.Errorf("name cannot be empty in named domain key: %v", name)
		}

		if domainType == model.Global {
			if len(parts) != 2 {
				return nil, fmt.Errorf("invalid global domain named key (expected Name:UUID): %v", name)
			}
			key, err := model.ParseKey(parts[1])
			if err != nil {
				return nil, fmt.Errorf("invalid named key %v: %w", parts[1], err)
			}
			keys = append(keys, model.NamedDomainKey{
				Name: parts[0],
				Key: model.DomainKey{
					Key: key,
				},
			})
		} else {
			if len(parts) != 3 {
				return nil, fmt.Errorf("invalid regional domain named key (expected Name:Region:UUID): %v", name)
			}

			if parts[1] == "" {
				return nil, fmt.Errorf("region cannot be empty in regional domain named key: %v", name)
			}

			region := model.Region(parts[1])
			if len(allowedRegions) > 0 && !slices.Contains(allowedRegions, region) {
				return nil, fmt.Errorf("invalid region for named key %v, expected one of: %v", region, allowedRegions)
			}

			key, err := model.ParseKey(parts[2])
			if err != nil {
				return nil, fmt.Errorf("invalid named key %v: %w", parts[2], err)
			}

			keys = append(keys, model.NamedDomainKey{
				Name: parts[0],
				Key: model.DomainKey{
					Region: region,
					Key:    key,
				},
			})
		}
	}
	return keys, nil
}
