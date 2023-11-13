package cmd

import (
	"context"
	"go.atoms.co/splitter/lib/service/location"
	"go.atoms.co/lib/signalx"
	"go.atoms.co/splitter/pkg/model"
	splitter "go.atoms.co/splitter/pkg/model"
	"fmt"
	"github.com/spf13/cobra"
)

func joinCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "join <tenant>/<service>/<domain> <region>",
		Short:        "Join service work distribution",
		Args:         cobra.ExactArgs(2),
		SilenceUsage: true,
	}

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		name, ok := splitter.ParseQualifiedDomainNameStr(args[0])
		if !ok {
			return fmt.Errorf("invalid qualified domain name: %v", args[0])
		}
		region := splitter.Region(args[1])

		return withClient(func(ctx context.Context, client model.Client) error {
			wctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			instance := model.NewInstance(location.NewInstance(location.New(region, "local")), "localhost")
			clusters, quit := client.Join(wctx, instance, name.Service, []splitter.QualifiedDomainName{name},
				func(ctx context.Context, shard splitter.Shard, lease splitter.Lease) {
					fmt.Println(fmt.Sprintf("Received shard %v with lease %v", shard, lease))
					<-ctx.Done()
					fmt.Println(fmt.Sprintf("Lost shard %v", shard))
				})
			for {
				select {
				case cluster := <-clusters:
					fmt.Println("Received cluster, ", cluster)
				case sig := <-signalx.InterruptChan():
					fmt.Println(fmt.Sprintf("Received '%v' signal. Draining Consumer.", sig))
					cancel()
					<-quit.Closed() // wait for drain

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
