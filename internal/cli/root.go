package cli

import "github.com/spf13/cobra"

func NewRootCmd(version, commit, date string) *cobra.Command {
	root := &cobra.Command{
		Use:           "mbforge",
		Short:         "Build a libSQL MusicBrainz metadata database from JSON dumps",
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	root.AddCommand(newBuildCmd(), newInfoCmd(), newSearchCmd(), newSearchIndexCmd(), newVersionCmd(version, commit, date))
	return root
}
