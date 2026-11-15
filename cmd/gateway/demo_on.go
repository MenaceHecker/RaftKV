//go:build fakecluster

package main

import (
	"flag"
	"log/slog"

	"github.com/MenaceHecker/raftkv/internal/gateway"
	"github.com/MenaceHecker/raftkv/internal/gateway/fakecluster"
)

// Built with -tags fakecluster only: a development binary with an in-memory
// DEMO cluster behind --fake-cluster. Never ship it.

func defineDemoFlags(fs *flag.FlagSet, s *settings) {
	fs.BoolVar(&s.fakeCluster, "fake-cluster", false, "serve an in-memory DEMO cluster instead of a real one (build tag fakecluster)")
}

func demoCluster(s *settings, logger *slog.Logger) (gateway.Cluster, bool) {
	if !s.fakeCluster {
		return nil, false
	}
	logger.Warn("serving an in-memory DEMO cluster, not a real one")
	return fakecluster.New(), true
}
