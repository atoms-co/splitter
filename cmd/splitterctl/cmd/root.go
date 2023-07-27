package cmd

import (
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
)

var (
	rootCmd = &cobra.Command{
		Use:   "splitterctl",
		Short: "KEQ2 command line client",
	}
)

func init() {
	rootCmd.PersistentFlags().StringVarP(&endpoint, "endpoint", "e", "localhost:50051", "Server endpoint, such as localhost:50051")
	rootCmd.PersistentFlags().DurationVar(&dialTimeout, "dial-timeout", 5*time.Second, "Dial timeout for connections")
	rootCmd.PersistentFlags().BoolVar(&insecure, "insecure", false, "Insecure connection")

	rootCmd.AddCommand(raftCmd)
	raftCmd.AddCommand(makeJoinRaftCommand())
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
