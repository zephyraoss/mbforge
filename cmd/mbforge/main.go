package main

import (
	"log"
	"os"

	"github.com/zephyraoss/mbforge/internal/cli"
)

var (
	version = "dev"
	commit  = "unknown"
	date    = "unknown"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	root := cli.NewRootCmd(version, commit, date)
	if err := root.Execute(); err != nil {
		log.Printf("error: %v", err)
		os.Exit(1)
	}
}
