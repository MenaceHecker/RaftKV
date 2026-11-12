//go:build faultinject

package faultinject

import (
	"fmt"
	"os"
	"strings"
)

// EnvVar lists the switches to enable at startup, comma separated, so a
// failing seed can be reproduced with `RAFTKV_FAULTINJECT=skip-noop chaosrun ...`.
const EnvVar = "RAFTKV_FAULTINJECT"

// The switches are plain booleans: set them before a run and never while one
// is in progress. The packages that read them forbid sync/atomic, and the
// simulator runs on one goroutine.
var enabled [count]bool

// Active reports whether a bug is injected.
func Active(s Switch) bool {
	return s >= 0 && s < count && enabled[s]
}

// Set turns one switch on or off.
func Set(s Switch, on bool) {
	if s < 0 || s >= count {
		panic(fmt.Sprintf("faultinject: unknown switch %d", int(s)))
	}
	enabled[s] = on
}

// Reset turns every switch off.
func Reset() {
	enabled = [count]bool{}
}

// Enable turns on the switches named in a comma separated list. An unknown
// name is an error and enables nothing, so a typo cannot run a clean build.
func Enable(list string) error {
	var parsed []Switch
	for _, name := range strings.Split(list, ",") {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		s, ok := Lookup(name)
		if !ok {
			return fmt.Errorf("faultinject: unknown switch %q", name)
		}
		parsed = append(parsed, s)
	}
	for _, s := range parsed {
		enabled[s] = true
	}
	return nil
}

func init() {
	if v := os.Getenv(EnvVar); v != "" {
		if err := Enable(v); err != nil {
			panic(err)
		}
	}
}
