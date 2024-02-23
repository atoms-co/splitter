package cmd

import (
	"context"
	"go.atoms.co/splitter/lib/service/location"
	"go.atoms.co/lib/signalx"
	"go.atoms.co/splitter/pkg/model"
	splitter "go.atoms.co/splitter/pkg/model"
	"fmt"
	"github.com/spf13/cobra"
	"time"
)

func joinCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "join <tenant>/<service>",
		Short:        "Join service work distribution",
		Args:         cobra.ExactArgs(1),
		SilenceUsage: true,
	}
	region := cmd.Flags().String("region", "global", "Consumer region (optional)")

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		name, ok := splitter.ParseQualifiedServiceNameStr(args[0])
		if !ok {
			return fmt.Errorf("invalid qualified service name: %v", args[0])
		}

		return withClient(func(ctx context.Context, client model.Client) error {
			wctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			instance := model.NewInstance(location.NewInstance(location.New(location.Region(*region), "local")), "localhost")
			clusters, quit := client.Join(wctx, instance, name, nil,
				func(ctx context.Context, id splitter.GrantID, shard splitter.Shard, ownership splitter.Ownership) {
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
				})

			signal := signalx.InterruptChan()
			for {
				select {
				case cluster := <-clusters:
					fmt.Println("Received cluster, ", cluster)
				case sig := <-signal:
					fmt.Println(fmt.Sprintf("Received '%v' signal. Draining Consumer.", sig))
					cancel()

					time.Sleep(100 * time.Millisecond)

					select {
					case <-quit.Closed(): // wait for drain
						fmt.Println("Consumer drained. Exiting")
					case <-signal: // or 2nd termination
					}

					return nil
				case <-quit.Closed():
					fmt.Println("Consumer closed. Exiting")
					time.Sleep(100 * time.Millisecond)
					return nil
				}
			}
		})
	}
	return cmd
}
