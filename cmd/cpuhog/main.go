// Command cpuhog burns CPU on n goroutines until it is killed. scripts/flake-guard.sh runs it
// next to the tests (LOAD=1) to approximate the contention of several packages' tests sharing
// the 2-core CI runner, which is where timer-sensitive tests in internal/gateway and
// internal/transport have flaked. It is deliberately outside internal/raft.
package main

import (
	"flag"
	"fmt"
	"os"
	"sync"
)

// hog starts n goroutines that spin until stop is closed. It returns once all n are running,
// and the returned WaitGroup is done when they have all observed stop.
func hog(n int, stop <-chan struct{}) *sync.WaitGroup {
	var done, started sync.WaitGroup
	done.Add(n)
	started.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer done.Done()
			started.Done()
			for !stopped(stop) {
				continue // spin: the point of this command is to burn CPU
			}
		}()
	}
	started.Wait()
	return &done
}

func stopped(stop <-chan struct{}) bool {
	select {
	case <-stop:
		return true
	default:
		return false
	}
}

func main() {
	n := flag.Int("n", 2, "busy goroutines")
	flag.Parse()
	if *n < 1 {
		_, _ = fmt.Fprintln(os.Stderr, "cpuhog: -n must be at least 1")
		os.Exit(2)
	}
	hog(*n, make(chan struct{})).Wait() // never stops: the caller kills the process
}
