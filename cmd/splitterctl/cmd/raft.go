package cmd

import (
	"context"
	splitter "go.atoms.co/splitter/pkg/model"
	"github.com/spf13/cobra"
)

var (
	raftCmd = &cobra.Command{
		Use:   "raft",
		Short: "Manage Tenants",
	}
)

func makeJoinRaftCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "join <addr> <id>",
		Short:        "join raft group",
		Args:         cobra.ExactArgs(2),
		SilenceUsage: true,
	}

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		//addr := args[0]
		//id := args[1]
		return withClient(func(ctx context.Context, client splitter.Client) error {
			//err := client.DeleteTenant(ctx, tenant)
			//if err != nil {
			//	return fmt.Errorf("delete tenant failed: %v", err)
			//}
			//fmt.Println("Deleted tenant: ", tenant)
			return nil
		})
	}

	return cmd
}
