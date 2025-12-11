package main

import (
	"fmt"
	"os"
	"path/filepath"
	"text/tabwriter"

	"github.com/openshift/microshift/etcd/pkg/etcdmembers"
	"github.com/openshift/microshift/pkg/config"
	"github.com/spf13/cobra"
)

func NewMemberCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "member",
		Short: "Manage etcd cluster members",
		Run: func(cmd *cobra.Command, args []string) {
			_ = cmd.Help()
		},
	}

	cmd.AddCommand(NewMemberListCommand())
	return cmd
}

func NewMemberListCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List etcd cluster members from local database",
		RunE: func(cmd *cobra.Command, args []string) error {
			dataDir := filepath.Join(config.DataDir, "etcd")
			members, err := etcdmembers.ReadMembersFromDB(dataDir)
			if err != nil {
				return fmt.Errorf("failed to read members: %w", err)
			}

			if len(members) == 0 {
				fmt.Println("No members found (database may not exist yet)")
				return nil
			}

			w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
			fmt.Fprintln(w, "ID\tNAME\tPEER URL\tCLIENT URL")
			for _, m := range members {
				peerURL := ""
				if len(m.PeerURLs) > 0 {
					peerURL = m.PeerURLs[0]
				}
				clientURL := ""
				if len(m.ClientURLs) > 0 {
					clientURL = m.ClientURLs[0]
				}
				fmt.Fprintf(w, "%x\t%s\t%s\t%s\n", m.ID, m.Name, peerURL, clientURL)
			}
			w.Flush()
			return nil
		},
	}
	return cmd
}
