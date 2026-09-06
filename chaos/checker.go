package chaos

import (
	"fmt"
	"math"
	"sort"
	"strings"
)

// Linearizability checking.
//
// Convergence is a weak claim. Every node agreeing on the same wrong answer is
// convergence, and so is a cluster that silently lost a write everyone had
// already been told succeeded. What a client actually depends on is stronger:
// that the whole system behaved as though each operation took effect
// instantaneously at some single moment between when it was invoked and when
// it returned, and that those moments form an order every client could agree
// on.
//
// That is linearizability, and it is checkable after the fact from nothing but
// a record of what was asked and what came back. This is where the chaos
// scenarios stop asking "did it crash" and start asking "were the answers
// right".
//
// The search is Wing and Gong's: try to linearize an operation that could
// legally go next, recurse, and backtrack when the model rejects it. Two
// things keep it tractable. Histories are split per key first, because a
// history over independent registers is linearizable exactly when each
// register's sub-history is — so the exponential search runs over a handful of
// operations rather than all of them. And states already explored are
// memoized, because the same set of linearized operations always leaves the
// model in the same place however it was reached.

// Verdict is the outcome of a check.
type Verdict int

const (
	// Linearizable means an ordering exists that explains every observed
	// result.
	Linearizable Verdict = iota

	// NotLinearizable means no such ordering exists. This is a genuine
	// correctness violation, not a limitation of the search.
	NotLinearizable

	// Undecided means the search ran out of budget before finishing.
	//
	// It is deliberately distinct from NotLinearizable. Reporting "I could not
	// decide" as "I found a violation" would be the worst possible failure for
	// a tool like this: it would cry wolf, and the first few false alarms
	// would teach everyone to ignore the real one.
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

// Result is a check's outcome, with enough detail to act on a failure.
type Result struct {
	Verdict Verdict

	// Key is the register whose sub-history could not be explained, when the
	// verdict is NotLinearizable.
	Key string

	// Ops is the sub-history for that key, in invocation order. It is what a
	// reader needs to see the violation for themselves.
	Ops []Op

	// Explored counts the search steps taken, which is the honest measure of
	// how hard the history was to decide.
	Explored int
}

// OK reports whether the history was found linearizable.
func (r Result) OK() bool { return r.Verdict == Linearizable }

// String renders the result, including the offending sub-history when there is
// one. A verdict with no evidence would be an assertion rather than a finding.
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

// DefaultBudget bounds the search.
//
// It exists so a pathological history reports Undecided rather than hanging.
// The shape of a history matters far more than its length: measured against
// this budget, 500 overlapping write/read pairs on one key decide in about a
// thousand states, while a dozen writes that are all concurrent with each
// other and cannot be explained take over a hundred thousand, and sixteen such
// writes exhaust the budget entirely.
//
// That is inherent rather than a weakness of this implementation — deciding
// linearizability is NP-complete in general — and it is why Undecided is a
// verdict rather than an error. Scenarios that want a decidable history should
// keep the number of simultaneously in-flight writes to one key modest, which
// is also what a real client does.
const DefaultBudget = 2_000_000

// Check reports whether a history is linearizable.
//
// Operations that definitely did not happen are dropped: they constrain
// nothing. Operations whose outcome is unknown are kept but treated as
// optional — the search may place them anywhere after their invocation or
// leave them out entirely, because that is exactly the ambiguity a client
// faces when it never hears back.
func Check(history []Op) Result {
	return CheckWithBudget(history, DefaultBudget)
}

// CheckWithBudget is Check with an explicit search bound.
func CheckWithBudget(history []Op, budget int) Result {
	byKey := make(map[string][]Op)
	for _, op := range history {
		if op.Status == StatusFailed {
			// It definitely did not happen, so it cannot constrain an
			// ordering. Keeping it would rule out orderings that are real.
			continue
		}
		if op.Status == StatusPending {
			// The caller stopped without settling this one. Treat it the same
			// as unknown rather than guessing.
			op.Status = StatusUnknown
		}
		byKey[op.Key] = append(byKey[op.Key], op)
	}

	// Keys are checked in a fixed order so a failure names the same key on
	// every run of the same history.
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

// value is the model: one register, either absent or holding a string.
type value struct {
	present bool
	data    string
}

// checkKey decides one register's sub-history.
func checkKey(key string, ops []Op, budget int) Result {
	if budget <= 0 {
		return Result{Verdict: Undecided, Key: key, Ops: ops}
	}

	// An unknown operation may or may not have taken effect, so it is given an
	// unbounded return time: it can be placed anywhere after its invocation,
	// and the search is allowed to finish without placing it at all.
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

// outcome is what one branch of the search concluded.
type outcome int

const (
	noOrdering outcome = iota
	foundOrdering
	exhaustedBudget
)

// search holds the state of one register's exploration.
type search struct {
	ops  []Op
	ends []int64
	done []bool

	// required is how many operations must be placed. Unknown operations are
	// not counted: an ordering that explains every definite result is a valid
	// explanation whether or not the ambiguous ones are included.
	required int

	budget   int
	explored int

	// memo records (set of placed operations, model value) pairs already
	// explored. Reaching the same set by a different route always leaves the
	// model in the same place, so there is nothing new to learn from it — and
	// without this the search is factorial rather than merely exponential.
	memo    map[string]struct{}
	bitmask []byte
}

// explore attempts to place the remaining operations, given the model's
// current value and how many required operations have been placed.
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

	// The earliest return among unplaced operations. Anything invoked after
	// that point cannot go next, because the operation that returned first
	// must be ordered before it — that real-time constraint is the entire
	// difference between linearizability and mere sequential consistency.
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
			// The model rejects this operation here, which usually means a
			// read observing a value no ordering could produce at this point.
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

// apply runs one operation against the model, reporting the resulting value
// and whether the operation was legal there.
func apply(current value, op Op) (value, bool) {
	if op.Kind == OpWrite {
		// A write is always legal; it simply replaces the value.
		return value{present: true, data: op.Value}, true
	}

	// A read is legal only if it observed exactly what the model holds.
	if op.Found != current.present {
		return current, false
	}
	if op.Found && op.Value != current.data {
		return current, false
	}
	return current, true
}

// stateKey builds the memo key for the current position: which operations have
// been placed, plus the model's value.
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
