package main

import "testing"

// The hog must start every goroutine it was asked for and stop them all when told to;
// a hog that cannot be stopped would outlive the guard's test run.
func TestHogStartsNAndStops(_ *testing.T) {
	stop := make(chan struct{})
	done := hog(3, stop) // returns only once all 3 are running
	close(stop)
	done.Wait() // returns only once all 3 observed stop
}
