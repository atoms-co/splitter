package cmd

import (
	"context"
	"go.atoms.co/lib/backoffx"
	"go.atoms.co/slicex"
	"go.atoms.co/lib/stringx"
	"go.atoms.co/splitter/pkg/core"
	"go.atoms.co/splitter/pkg/model"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/spf13/cobra"
	"slices"
	"time"
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

func makeCoordinatorRevokeCmd() *cobra.Command {
	grantsPerCycle := 0
	delay := ""
	var rawRegions []string
	var domains []string
	var grantIds []string

	const (
		flagDisableLb  = "disable-load-balance"
		flagDryRun     = "dry-run"
		flagMisaligned = "misaligned"
	)

	cmd := &cobra.Command{
		Use:          "revoke <tenant>/<service>",
		Short:        "revoke grants matching provided filters, grants will be reallocated to new workers",
		Args:         cobra.ExactArgs(1),
		SilenceUsage: true,
	}
	// operational flags
	cmd.Flags().IntVar(&grantsPerCycle, "per-cycle", 1, "grants to revoke per cycle")
	cmd.Flags().StringVar(&delay, "delay", "30s", "delay between revocation cycles")
	cmd.Flags().Bool(flagDisableLb, true, "disable load balance before grants are revoked")
	cmd.Flags().Bool(flagDryRun, false, "Print the grants that match the provided filters and return")

	// grant filters
	cmd.Flags().StringArrayVarP(&rawRegions, "region", "r", rawRegions, "region filter")
	cmd.Flags().StringArrayVarP(&domains, "domain", "d", domains, "domain filter")
	cmd.Flags().StringArrayVarP(&grantIds, "grant-id", "g", grantIds, "grant id filter")
	cmd.Flags().Bool(flagMisaligned, false, "filter for misaligned grants")

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		service, ok := model.ParseQualifiedServiceNameStr(args[0])
		if !ok {
			return fmt.Errorf("invalid qualified service name: %v", args[0])
		}
		domainsFilter := slicex.Map(domains,
			func(domain string) model.QualifiedDomainName {
				return model.QualifiedDomainName{
					Service: service,
					Domain:  stringx.FromString[model.DomainName](domain),
				}
			})
		regionsFilter := slicex.Map(rawRegions, stringx.FromString[model.Region])
		grantIDFilter := slicex.Map(grantIds, stringx.FromString[model.GrantID])

		disableLb, err := cmd.Flags().GetBool(flagDisableLb)
		if err != nil {
			return fmt.Errorf("invalid disable load balance flag: %v", err)
		}

		dryRun, err := cmd.Flags().GetBool(flagDryRun)
		if err != nil {
			return fmt.Errorf("invalid dry-run flag: %v", err)
		}

		misalignedOnly, err := cmd.Flags().GetBool(flagMisaligned)
		if err != nil {
			return fmt.Errorf("invalid misaligned flag: %v", err)
		}

		delayDuration, err := time.ParseDuration(delay)
		if err != nil {
			return fmt.Errorf("invalid delay duration: %v", delay)
		}

		return withInternalClient(func(ctx context.Context, client core.Client) error {
			// (1) Disable load balance before retrieving snapshot if specified
			resetLB := false

			if disableLb && !dryRun {
				wasDisabled, err := coerceLoadBalanceOperational(service, true)
				if err != nil {
					return err
				}
				resetLB = !wasDisabled
			}

			// (2) Retrieve snapshot and apply specified grant filters

			_, snapshot, err := client.CoordinatorInfo(ctx, service)
			if err != nil {
				return fmt.Errorf("error getting coordinator info: %w", err)
			}

			var filtered []consumerGrant
			for _, assignment := range snapshot.Assignments() {
				c := assignment.Consumer()
				region := c.Instance().Location().Region
				if len(regionsFilter) > 0 && !slices.Contains(regionsFilter, region) {
					continue
				}
				for _, g := range assignment.Grants() {
					passedDomainFilter := len(domainsFilter) == 0 || slices.Contains(domainsFilter, g.Shard().Domain)
					passedMisalignedFilter := !misalignedOnly || (g.Shard().Type == model.Regional && g.Shard().Region != region)
					passedGrantFilter := len(grantIDFilter) == 0 || slices.Contains(grantIDFilter, g.ID())

					if passedDomainFilter && passedMisalignedFilter && passedGrantFilter {
						filtered = append(filtered, consumerGrant{
							consumerId: c.ID(),
							grantId:    g.ID(),
							domain:     g.Shard().Domain,
						})
					}
				}
			}

			fmt.Printf("Found %v grant(s)\n", len(filtered))

			if dryRun {
				for _, cg := range filtered {
					fmt.Printf("Found: domain=%v, consumer=%v grant=%v\n", cg.domain, cg.consumerId, cg.grantId)
				}
				return nil
			}

			if len(filtered) == 0 {
				if resetLB {
					_, err := coerceLoadBalanceOperational(service, false)
					return err
				}
				return nil
			}

			// (3) Begin Revoking Grants

			for len(filtered) > 0 {
				grants := map[model.ConsumerID][]model.GrantID{}

				i := min(grantsPerCycle, len(filtered))
				batch := filtered[0:i]
				filtered = filtered[i:]

				for _, cg := range batch {
					grants[cg.consumerId] = append(grants[cg.consumerId], cg.grantId)
				}

				err := client.CoordinatorRevokeGrants(ctx, service, grants)
				if err != nil {
					err = fmt.Errorf("error revoking grants: %w", err)
					break
				}
				for c, gs := range grants {
					for _, g := range gs {
						fmt.Printf("revoked consumer=%v grant=%v\n", c, g)
					}
				}
				time.Sleep(delayDuration)
			}

			// (4) Restore load balance operational config

			if resetLB {
				_, err2 := coerceLoadBalanceOperational(service, false)
				err = errors.Join(err, err2)
			}
			return err
		})

	}

	return cmd
}

type consumerGrant struct {
	consumerId model.ConsumerID
	grantId    model.GrantID
	domain     model.QualifiedDomainName
}

func coerceLoadBalanceOperational(service model.QualifiedServiceName, disableLB bool) (bool, error) {
	var prev bool
	err := withClient(func(ctx context.Context, client model.Client) error {
		var err error
		b := backoffx.NewLimited(10*time.Second, backoffx.WithMaxRetries(5))
		prev, err = backoffx.Retry1(b, func() (bool, error) {
			info, err := client.InfoService(ctx, service)
			if err != nil {
				return false, err
			}
			version := info.Info().Version()
			prev := info.Info().Service().Operational().DisableLoadBalance()

			if prev == disableLB {
				return prev, nil
			}

			op := model.WithUpdateServiceOperational(model.NewServiceOperational(model.WithServiceOperationalDisableLoadBalance(disableLB)))
			_, err = client.UpdateService(ctx, service, version, op)
			return prev, err
		})
		if err != nil {
			return err
		}
		if prev != disableLB {
			fmt.Printf("Updated %v disable load balance operational to '%v'\n", service, disableLB)
		}
		return nil
	})
	return prev, err
}

func makeCoordinatorClusterSyncCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "sync <tenant>/<service>",
		Short:        "Sync coordinator cluster to consumers",
		Args:         cobra.ExactArgs(1),
		SilenceUsage: true,
	}

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		name, ok := model.ParseQualifiedServiceNameStr(args[0])
		if !ok {
			return fmt.Errorf("invalid qualified service name: %v", args[0])
		}

		return withInternalClient(func(ctx context.Context, client core.Client) error {
			if err := client.CoordinatorClusterSync(ctx, name); err != nil {
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
