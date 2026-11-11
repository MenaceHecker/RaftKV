package chaos

import (
	"fmt"
	"strconv"
	"strings"
)

// MaxSeeds bounds how many seeds one specification may expand to.
const MaxSeeds = 100000

// ParseSeeds expands a seed specification such as "1-200", "7" or "1,3,5-9"
// into the seeds to run, in the order given. Seeds start at 1 (RUNBOOK 8.1: a
// failure must be reproducible from the seed it prints).
func ParseSeeds(spec string) ([]int64, error) {
	if strings.TrimSpace(spec) == "" {
		return nil, fmt.Errorf("chaos: the seed specification is empty")
	}

	var out []int64
	for _, part := range strings.Split(spec, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			return nil, fmt.Errorf("chaos: %q has an empty element", spec)
		}

		lo, hi, isRange := strings.Cut(part, "-")
		if !isRange {
			n, err := parseSeed(part)
			if err != nil {
				return nil, err
			}
			if len(out)+1 > MaxSeeds {
				return nil, tooMany(spec)
			}
			out = append(out, n)
			continue
		}

		if lo == "" || hi == "" {
			return nil, fmt.Errorf("chaos: malformed range %q, want LOW-HIGH", part)
		}
		a, err := parseSeed(lo)
		if err != nil {
			return nil, err
		}
		b, err := parseSeed(hi)
		if err != nil {
			return nil, err
		}
		if b < a {
			return nil, fmt.Errorf("chaos: range %q is descending", part)
		}
		if b-a+1 > MaxSeeds || int64(len(out))+(b-a+1) > MaxSeeds {
			return nil, tooMany(spec)
		}
		for s := a; s <= b; s++ {
			out = append(out, s)
		}
	}
	return out, nil
}

func tooMany(spec string) error {
	return fmt.Errorf("chaos: %q expands to too many seeds (the limit is %d)", spec, MaxSeeds)
}

func parseSeed(s string) (int64, error) {
	n, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("chaos: invalid seed %q", s)
	}
	if n < 1 {
		return 0, fmt.Errorf("chaos: seed %d must be at least 1", n)
	}
	return n, nil
}
