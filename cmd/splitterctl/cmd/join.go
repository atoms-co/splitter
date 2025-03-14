package cmd

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"

	"go.atoms.co/splitter/lib/service/location"
	"go.atoms.co/lib/signalx"
	"go.atoms.co/splitter/pkg/model"
)

func joinCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "join <tenant>/<service>",
		Short:        "Join service work distribution",
		Args:         cobra.ExactArgs(1),
		SilenceUsage: true,
	}
	region := cmd.Flags().String("region", "global", "Consumer region (optional)")
	keyNames := cmd.Flags().StringSlice("key-names", []string{}, "Canary domain key names (optional). e.g. porc/ruff")
	capacity := cmd.Flags().Int("capacity-limit", 0, "Maximum number of shards that can be assigned")

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		name, ok := model.ParseQualifiedServiceNameStr(args[0])
		if !ok {
			return fmt.Errorf("invalid qualified service name: %v", args[0])
		}

		var opts []model.ConsumerOption
		if len(*keyNames) > 0 {
			var d []model.DomainKeyName
			for _, keyName := range *keyNames {
				parsed, ok := model.ParseDomainKeyNameStr(keyName)
				if !ok {
					return fmt.Errorf("invalid domain key name: %v", keyName)
				}
				d = append(d, parsed)
			}
			opts = append(opts, model.WithKeyNames(d...))
		}
		if *capacity > 0 {
			opts = append(opts, model.WithCapacityLimit(*capacity))
		}

		return withConsumerClient(func(ctx context.Context, client model.ConsumerClient) error {
			wctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			instance := model.NewInstance(location.NewInstance(location.New(location.Region(*region), "local")), "localhost")
			clusters, quit := client.Join(wctx, instance, name,
				func(ctx context.Context, id model.GrantID, shard model.Shard, ownership model.Ownership) {
					fmt.Println(fmt.Sprintf("Received shard %v with lease %v", shard, ownership.Expiration()))

					select {
					case <-ownership.Revoked().Closed():
						fmt.Println(fmt.Sprintf("Shard %v revoked", shard))
					case <-ownership.Expired().Closed():
						fmt.Println(fmt.Sprintf("Shard %v expired", shard))
					case <-ctx.Done():
						fmt.Println(fmt.Sprintf("Shard %v closed", shard))
					}

					fmt.Println(fmt.Sprintf("Lost shard %v", shard))
				}, opts...)

			signal := signalx.InterruptChan()
			for {
				select {
				case cluster := <-clusters:
					fmt.Println("Received cluster, ", cluster)
				case sig := <-signal:
					fmt.Println(fmt.Sprintf("Received '%v' signal. Draining Consumer.", sig))
					cancel()
					select {
					case <-quit.Closed(): // wait for drain
						fmt.Println("Consumer drained. Exiting")
					case <-signal: // or 2nd termination
					}

					return nil
				case <-quit.Closed():
					fmt.Println("Consumer closed. Exiting")
					return nil
				}
			}
		})
	}
	return cmd
}
