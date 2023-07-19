package cmd

import (
	"context"
	"flag"
	"fmt"
	"os"

	"go.atoms.co/lib/service/logx"
	"github.com/spf13/cobra"
)

var (
	rootCmd = &cobra.Command{
		Use:   "splitter",
		Short: "splitter command line",
	}
)

func init() {
	rootCmd.PersistentFlags().AddGoFlagSet(flag.CommandLine)
	rootCmd.PersistentPreRun = func(cmd *cobra.Command, args []string) {
		logx.Init(context.Background())
	}

	rootCmd.AddCommand(makeStartCommand())

}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
