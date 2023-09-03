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
		Short: "Splitter command line client",
	}
)

func init() {
	rootCmd.PersistentFlags().StringVarP(&endpoint, "endpoint", "e", "localhost:50051", "Server endpoint, such as localhost:50051")
	rootCmd.PersistentFlags().DurationVar(&dialTimeout, "dial-timeout", 5*time.Second, "Dial timeout for connections")
	rootCmd.PersistentFlags().BoolVar(&insecure, "insecure", false, "Insecure connection")

	rootCmd.AddCommand(tenantCmd)
	tenantCmd.AddCommand(makeListTenantCmd())
	tenantCmd.AddCommand(makeNewTenantCmd())
	tenantCmd.AddCommand(makeReadTenantCmd())
	tenantCmd.AddCommand(makeUpdateTenantCmd())
	tenantCmd.AddCommand(makeDeleteTenantCmd())

	rootCmd.AddCommand(domainCmd)
	domainCmd.AddCommand(makeListDomainCmd())
	domainCmd.AddCommand(makeNewDomainCmd())
	domainCmd.AddCommand(makeReadDomainCmd())
	domainCmd.AddCommand(makeUpdateDomainCmd())
	domainCmd.AddCommand(makeDeleteDomainCmd())

	rootCmd.AddCommand(placementCmd)
	placementCmd.AddCommand(makeListPlacementCmd())
	placementCmd.AddCommand(makeNewPlacementCmd())
	placementCmd.AddCommand(makeInfoPlacementCmd())
	placementCmd.AddCommand(makeUpdatePlacementCmd())
	placementCmd.AddCommand(makeDeletePlacementCmd())

	placementCmd.AddCommand(makePublicInfoPlacementCmd())
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
