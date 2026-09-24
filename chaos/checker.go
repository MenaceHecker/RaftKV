package chaos

import (
	"fmt"
	"math"
	"sort"
	"strings"
)

type Verdict int

const (
	Linearizable Verdict = iota

	NotLinearizable

	Undecided
)

func (v Verdict) String() string {
	switch v {
	case Linearizable:
		return "linearizable"
	case NotLinearizable:
		return "not linearizable"
	default:
		return "undecided"
	}
}

type Result struct {
	Verdict Verdict

	Key string

	Ops []Op

	Explored int
}

func (r Result) OK() bool { return r.Verdict == Linearizable }

func (r Result) String() string {
	if r.Verdict == Linearizable {
		return fmt.Sprintf("linearizable (%d states explored)", r.Explored)
	}
	if r.Verdict == Undecided {
		return fmt.Sprintf("undecided after %d states; the history is too large to "+
			"decide exactly", r.Explored)
	}

	var b strings.Builder
	fmt.Fprintf(&b, "not linearizable on key %q after %d states explored:\n", r.Key, r.Explored)
	for _, op := range r.Ops {
		fmt.Fprintf(&b, "  client %d %-5s [%d,%d] %s",
			op.Client, op.Kind, op.Invoked, op.Returned, op.Status)
		if op.Kind == OpWrite {
			fmt.Fprintf(&b, " wrote %q\n", op.Value)
			continue
		}
		if op.Found {
			fmt.Fprintf(&b, " read %q\n", op.Value)
			continue
		}
		fmt.Fprint(&b, " read <absent>\n")
	}
	return b.String()
}

const DefaultBudget = 2_000_000

func Check(history []Op) Result {
	return CheckWithBudget(history, DefaultBudget)
}

func CheckWithBudget(history []Op, budget int) Result {
	byKey := make(map[string][]Op)
	for _, op := range history {
		if op.Status == StatusFailed {
			continue
		}
		if op.Status == StatusPending {
			op.Status = StatusUnknown
		}
		byKey[op.Key] = append(byKey[op.Key], op)
	}

	keys := make([]string, 0, len(byKey))
	for k := range byKey {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	total := Result{Verdict: Linearizable}
	for _, key := range keys {
		ops := byKey[key]
		sort.SliceStable(ops, func(i, j int) bool { return ops[i].Invoked < ops[j].Invoked })

		res := checkKey(key, ops, budget-total.Explored)
		total.Explored += res.Explored
		if res.Verdict != Linearizable {
			res.Explored = total.Explored
			return res
		}
	}
	return total
}

type value struct {
	present bool
	data    string
}

func checkKey(key string, ops []Op, budget int) Result {
	if budget <= 0 {
		return Result{Verdict: Undecided, Key: key, Ops: ops}
	}

	ends := make([]int64, len(ops))
	required := 0
	for i, op := range ops {
		if op.Status == StatusUnknown {
			ends[i] = math.MaxInt64
			continue
		}
		ends[i] = op.Returned
		required++
	}

	s := &search{
		ops:      ops,
		ends:     ends,
		done:     make([]bool, len(ops)),
		budget:   budget,
		memo:     make(map[string]struct{}),
		bitmask:  make([]byte, (len(ops)+7)/8),
		required: required,
	}

	switch s.explore(value{}, 0) {
	case foundOrdering:
		return Result{Verdict: Linearizable, Key: key, Explored: s.explored}
	case exhaustedBudget:
		return Result{Verdict: Undecided, Key: key, Ops: ops, Explored: s.explored}
	default:
		return Result{Verdict: NotLinearizable, Key: key, Ops: ops, Explored: s.explored}
	}
}

type outcome int

const (
	noOrdering outcome = iota
	foundOrdering
	exhaustedBudget
)

type search struct {
	ops  []Op
	ends []int64
	done []bool

	required int

	budget   int
	explored int

	memo    map[string]struct{}
	bitmask []byte
}

func (s *search) explore(current value, placed int) outcome {
	if placed == s.required {
		return foundOrdering
	}
	if s.explored >= s.budget {
		return exhaustedBudget
	}
	s.explored++

	if _, seen := s.memo[s.stateKey(current)]; seen {
		return noOrdering
	}

	earliest := int64(math.MaxInt64)
	for i := range s.ops {
		if !s.done[i] && s.ends[i] < earliest {
			earliest = s.ends[i]
		}
	}

	budgetGone := false
	for i := range s.ops {
		if s.done[i] || s.ops[i].Invoked > earliest {
			continue
		}

		next, ok := apply(current, s.ops[i])
		if !ok {
			continue
		}

		s.done[i] = true
		step := 0
		if s.ops[i].Status != StatusUnknown {
			step = 1
		}

		switch s.explore(next, placed+step) {
		case foundOrdering:
			s.done[i] = false
			return foundOrdering
		case exhaustedBudget:
			budgetGone = true
		}
		s.done[i] = false

		if budgetGone {
			return exhaustedBudget
		}
	}

	s.memo[s.stateKey(current)] = struct{}{}
	return noOrdering
}

func apply(current value, op Op) (value, bool) {
	if op.Kind == OpWrite {
		return value{present: true, data: op.Value}, true
	}

	if op.Found != current.present {
		return current, false
	}
	if op.Found && op.Value != current.data {
		return current, false
	}
	return current, true
}

func (s *search) stateKey(current value) string {
	for i := range s.bitmask {
		s.bitmask[i] = 0
	}
	for i, done := range s.done {
		if done {
			s.bitmask[i/8] |= 1 << (i % 8)
		}
	}

	var b strings.Builder
	b.Grow(len(s.bitmask) + len(current.data) + 2)
	b.Write(s.bitmask)
	if current.present {
		b.WriteByte(1)
	} else {
		b.WriteByte(0)
	}
	b.WriteString(current.data)
	return b.String()
}
