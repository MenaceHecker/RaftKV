//go:build !fakecluster

package main

import (
	"flag"
	"log/slog"

	"github.com/MenaceHecker/raftkv/internal/gateway"
)

// The production build has no demo cluster: the flag does not exist and the
// in-memory fake is not linked, so this binary can never serve fake data.

func defineDemoFlags(*flag.FlagSet, *settings) {}

func demoCluster(*settings, *slog.Logger) (gateway.Cluster, bool) { return nil, false }
