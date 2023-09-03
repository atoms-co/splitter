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
		Use:          "list <tenant>",
		Short:        "List domains",
		Args:         cobra.ExactArgs(1),
		SilenceUsage: true,
	}

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		tenant := splitter.TenantName(args[0])
		return withClient(func(ctx context.Context, client model.Client) error {
			list, err := client.ListDomains(ctx, tenant)
			if err != nil {
				return err
			}
			for _, info := range list {
				printJson(model.UnwrapDomainInfo(info), false)
			}
			return nil
		})
	}

	return cmd
}

func makeNewDomainCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "new <tenant>/<service>/<domain>",
		Short:        "New domain",
		Args:         cobra.ExactArgs(1),
		SilenceUsage: true,
	}

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		name, ok := splitter.ParseQualifiedDomainNameStr(args[0])
		if !ok {
			return fmt.Errorf("invalid qualified domain name: %v", args[0])
		}

		return withClient(func(ctx context.Context, client model.Client) error {
			domain, err := client.NewDomain(ctx, name, splitter.Unit, splitter.NewDomainConfig()) // TODO(jhhurwitz): 09/01/2023 Add domain customization
			if err != nil {
				return err
			}
			printJson(splitter.UnwrapDomain(domain), true)
			return nil
		})
	}

	return cmd
}

func makeReadDomainCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "read <tenant>/<service>/<domain>",
		Short:        "Show domain information",
		Args:         cobra.ExactArgs(1),
		SilenceUsage: true,
	}

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		name, ok := splitter.ParseQualifiedDomainNameStr(args[0])
		if !ok {
			return fmt.Errorf("invalid qualified domain name: %v", args[0])
		}

		return withClient(func(ctx context.Context, client model.Client) error {
			info, err := client.ReadDomain(ctx, name)
			if err != nil {
				return err
			}
			printJson(model.UnwrapDomainInfo(info), true)
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

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		_, ok := splitter.ParseQualifiedDomainNameStr(args[0])
		if !ok {
			return fmt.Errorf("invalid qualified domain name: %v", args[0])
		}

		return withClient(func(ctx context.Context, client model.Client) error {
			// TODO(jhhurwitz): 09/01/2023: Implement
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
