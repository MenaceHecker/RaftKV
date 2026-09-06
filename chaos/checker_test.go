package chaos

import (
	"fmt"
	"testing"
)

// Tests for the linearizability checker.
//
// The failure this guards against is a checker that accepts everything. It
// would pass every scenario, look like strong evidence, and mean nothing — so
// most of these tests hand it histories that are definitely wrong and require
// it to say so. Those matter far more than the ones it should accept.

// w builds a completed write.
func w(client int, key, value string, invoked, returned int64) Op {
	return Op{
		Kind: OpWrite, Client: client, Key: key, Value: value,
		Invoked: invoked, Returned: returned, Status: StatusOK,
	}
}

// r builds a completed read that found a value.
func r(client int, key, value string, invoked, returned int64) Op {
	return Op{
		Kind: OpRead, Client: client, Key: key, Value: value, Found: true,
		Invoked: invoked, Returned: returned, Status: StatusOK,
	}
}

// rAbsent builds a completed read that found nothing.
func rAbsent(client int, key string, invoked, returned int64) Op {
	return Op{
		Kind: OpRead, Client: client, Key: key, Found: false,
		Invoked: invoked, Returned: returned, Status: StatusOK,
	}
}

// unknown marks an operation as having an unknown outcome.
func unknown(op Op) Op {
	op.Status = StatusUnknown
	return op
}

// failed marks an operation as definitely not having happened.
func failed(op Op) Op {
	op.Status = StatusFailed
	return op
}

func assertLinearizable(t *testing.T, history []Op) {
	t.Helper()
	res := Check(history)
	if !res.OK() {
		t.Fatalf("expected a linearizable history, got %s", res)
	}
}

func assertNotLinearizable(t *testing.T, history []Op) {
	t.Helper()
	res := Check(history)
	if res.Verdict != NotLinearizable {
		t.Fatalf("expected a violation, got %s", res)
	}
}

func TestEmptyHistoryIsLinearizable(t *testing.T) {
	assertLinearizable(t, nil)
}

func TestSequentialHistoryIsLinearizable(t *testing.T) {
	assertLinearizable(t, []Op{
		w(1, "x", "a", 0, 1),
		r(1, "x", "a", 2, 3),
		w(1, "x", "b", 4, 5),
		r(1, "x", "b", 6, 7),
	})
}

func TestReadingAnAbsentKeyIsLinearizable(t *testing.T) {
	assertLinearizable(t, []Op{
		rAbsent(1, "x", 0, 1),
		w(1, "x", "a", 2, 3),
		r(1, "x", "a", 4, 5),
	})
}

func TestStaleReadIsRejected(t *testing.T) {
	// The violation this whole apparatus exists to catch. The read is invoked
	// after the second write returned, so no ordering can put it before that
	// write — and yet it observed the first write's value.
	//
	// This is precisely what a partitioned leader serving from its own state
	// would produce, which is why the read-index protocol exists.
	assertNotLinearizable(t, []Op{
		w(1, "x", "a", 0, 1),
		w(1, "x", "b", 2, 3),
		r(2, "x", "a", 4, 5),
	})
}

func TestReadingAValueFromTheFutureIsRejected(t *testing.T) {
	// The read returned before the write was even invoked, so no ordering can
	// place the write first.
	assertNotLinearizable(t, []Op{
		r(1, "x", "a", 0, 1),
		w(2, "x", "a", 2, 3),
	})
}

func TestReadingAValueNobodyWroteIsRejected(t *testing.T) {
	assertNotLinearizable(t, []Op{
		w(1, "x", "a", 0, 1),
		r(2, "x", "z", 2, 3),
	})
}

func TestReadingAbsentAfterAWriteIsRejected(t *testing.T) {
	// A committed write cannot vanish. This is the shape a lost write takes in
	// a history: everything looks fine except that the data is gone.
	assertNotLinearizable(t, []Op{
		w(1, "x", "a", 0, 1),
		rAbsent(2, "x", 2, 3),
	})
}

func TestGoingBackwardsIsRejected(t *testing.T) {
	// Two reads with no write between them observed different values, which no
	// ordering can produce.
	assertNotLinearizable(t, []Op{
		w(1, "x", "a", 0, 1),
		w(1, "x", "b", 2, 3),
		r(2, "x", "b", 4, 5),
		r(2, "x", "a", 6, 7),
	})
}

func TestConcurrentWritesMayBeOrderedEitherWay(t *testing.T) {
	// Two writes that overlap can be linearized in either order, so a read
	// that follows may legitimately observe either one. A checker that
	// insisted on invocation order would reject perfectly correct histories.
	assertLinearizable(t, []Op{
		w(1, "x", "a", 0, 10),
		w(2, "x", "b", 0, 10),
		r(3, "x", "a", 11, 12),
	})
	assertLinearizable(t, []Op{
		w(1, "x", "a", 0, 10),
		w(2, "x", "b", 0, 10),
		r(3, "x", "b", 11, 12),
	})
}

func TestAReadConcurrentWithAWriteMaySeeEitherValue(t *testing.T) {
	assertLinearizable(t, []Op{
		w(1, "x", "a", 0, 1),
		w(2, "x", "b", 2, 8),
		r(3, "x", "a", 3, 9),
	})
	assertLinearizable(t, []Op{
		w(1, "x", "a", 0, 1),
		w(2, "x", "b", 2, 8),
		r(3, "x", "b", 3, 9),
	})
}

func TestRealTimeOrderIsEnforcedAcrossClients(t *testing.T) {
	// The property that separates linearizability from sequential consistency.
	// Client 2's read is entirely after client 1's read returned, so it cannot
	// be ordered before it — and a value cannot un-write itself.
	assertNotLinearizable(t, []Op{
		w(1, "x", "a", 0, 1),
		w(1, "x", "b", 2, 3),
		r(2, "x", "b", 4, 5),
		r(3, "x", "a", 6, 7),
	})
}

func TestFailedOperationsAreIgnored(t *testing.T) {
	// An operation that definitely did not happen constrains nothing. Keeping
	// it would rule out orderings that actually occurred.
	assertLinearizable(t, []Op{
		w(1, "x", "a", 0, 1),
		failed(w(2, "x", "b", 2, 3)),
		r(3, "x", "a", 4, 5),
	})
}

func TestFailedWritesCannotExplainARead(t *testing.T) {
	// The other direction: dropping a failed write must not let its value
	// justify a read that observed it. If it did, the checker would excuse
	// exactly the bug where a refused write took effect anyway.
	assertNotLinearizable(t, []Op{
		failed(w(1, "x", "a", 0, 1)),
		r(2, "x", "a", 2, 3),
	})
}

func TestUnknownWritesMayOrMayNotHaveHappened(t *testing.T) {
	// A client that never heard back cannot tell a lost request from a lost
	// reply. Both readings have to be available to the search.

	// Here the unknown write must have taken effect, since the read saw it.
	assertLinearizable(t, []Op{
		unknown(w(1, "x", "a", 0, 5)),
		r(2, "x", "a", 6, 7),
	})

	// And here it must not have, since the read found nothing.
	assertLinearizable(t, []Op{
		unknown(w(1, "x", "a", 0, 5)),
		rAbsent(2, "x", 6, 7),
	})
}

func TestUnknownWritesCannotExcuseEverything(t *testing.T) {
	// Treating unknown operations as free would make the checker useless: any
	// history could be explained by inventing one. An unknown write still has
	// to be placed somewhere its value could have been observed.
	//
	// The read observes a value nobody ever wrote, unknown or otherwise.
	assertNotLinearizable(t, []Op{
		unknown(w(1, "x", "a", 0, 5)),
		r(2, "x", "z", 6, 7),
	})
}

func TestAnUnknownWriteCannotMoveBeforeItsInvocation(t *testing.T) {
	// Unknown operations get an unbounded return time, not an unbounded
	// invocation time. A write cannot take effect before the client asked for
	// it.
	assertNotLinearizable(t, []Op{
		r(1, "x", "a", 0, 1),
		unknown(w(2, "x", "a", 2, 9)),
	})
}

func TestKeysAreCheckedIndependently(t *testing.T) {
	// A history over independent registers is linearizable exactly when each
	// register's sub-history is. Splitting is what keeps the search tractable,
	// and it must not lose violations.
	assertLinearizable(t, []Op{
		w(1, "x", "a", 0, 1),
		w(1, "y", "b", 0, 1),
		r(2, "x", "a", 2, 3),
		r(2, "y", "b", 2, 3),
	})

	// A violation on one key must still be found when another key is fine.
	res := Check([]Op{
		w(1, "x", "a", 0, 1),
		r(2, "x", "a", 2, 3),
		w(1, "y", "b", 0, 1),
		w(1, "y", "c", 2, 3),
		r(2, "y", "b", 4, 5),
	})
	if res.Verdict != NotLinearizable {
		t.Fatalf("a violation on one key was missed: %s", res)
	}
	if res.Key != "y" {
		t.Fatalf("the violation was reported on key %q, want y", res.Key)
	}
}

func TestAViolationReportsItsEvidence(t *testing.T) {
	// A verdict with no evidence is an assertion. Whoever reads a failing
	// chaos run needs to see the operations for themselves.
	res := Check([]Op{
		w(1, "x", "a", 0, 1),
		w(1, "x", "b", 2, 3),
		r(2, "x", "a", 4, 5),
	})
	if res.Verdict != NotLinearizable {
		t.Fatalf("expected a violation, got %s", res)
	}
	if len(res.Ops) != 3 {
		t.Fatalf("the report carries %d operations, want the 3 in the sub-history", len(res.Ops))
	}

	text := res.String()
	for _, want := range []string{"not linearizable", `key "x"`, "wrote", "read"} {
		if !contains(text, want) {
			t.Fatalf("the report does not mention %q:\n%s", want, text)
		}
	}
}

func TestBudgetExhaustionIsUndecidedNotAViolation(t *testing.T) {
	// The distinction that keeps the tool trustworthy. Reporting "I could not
	// decide" as "I found a violation" would cry wolf, and the first few false
	// alarms would teach everyone to ignore the real one.
	// The history has to be genuinely hard, not merely large. A linearizable
	// one is often found greedily in a few steps however many operations it
	// has; what forces the search to explore is a history with no valid
	// ordering at all, so that every arrangement must be ruled out.
	var history []Op
	for i := range 12 {
		history = append(history, w(1, "x", fmt.Sprintf("v%d", i), 0, 1000))
	}
	// No write ever produced this value, so nothing can explain the read.
	history = append(history, r(2, "x", "never-written", 1001, 1002))

	// With an ample budget the answer is a definite violation.
	if got := Check(history); got.Verdict != NotLinearizable {
		t.Fatalf("with a full budget the verdict is %s, want a violation", got.Verdict)
	}

	res := CheckWithBudget(history, 20)
	if res.Verdict != Undecided {
		t.Fatalf("a starved search reported %s, want undecided", res.Verdict)
	}
	if res.OK() {
		t.Fatal("an undecided result reports itself as OK")
	}
}

func TestLargeCorrectHistoryIsAccepted(t *testing.T) {
	// The checker has to be usable on realistic histories, not only on
	// hand-written examples.
	var history []Op
	var t0 int64
	for i := range 200 {
		value := fmt.Sprintf("v%d", i)
		history = append(history, w(1, "x", value, t0, t0+1))
		history = append(history, r(2, "x", value, t0+2, t0+3))
		t0 += 4
	}

	res := Check(history)
	if !res.OK() {
		t.Fatalf("a large correct history was rejected: %s", res)
	}
}

func TestLargeConcurrentHistoryIsAccepted(t *testing.T) {
	// Overlapping operations across several keys, which is what the scenarios
	// actually produce.
	var history []Op
	var t0 int64
	for i := range 60 {
		key := fmt.Sprintf("k%d", i%5)
		value := fmt.Sprintf("v%d", i)
		history = append(history,
			w(1, key, value, t0, t0+3),
			w(2, key, value, t0+1, t0+4),
			r(3, key, value, t0+5, t0+6),
		)
		t0 += 7
	}

	res := Check(history)
	if !res.OK() {
		t.Fatalf("a large concurrent history was rejected: %s", res)
	}
}

func TestOneBadReadAmongManyIsCaught(t *testing.T) {
	// A violation buried in a long correct history must still be found. A
	// checker that gave up quietly on size would miss exactly the rare bug
	// these scenarios exist to surface.
	var history []Op
	var t0 int64
	for i := range 100 {
		value := fmt.Sprintf("v%d", i)
		history = append(history, w(1, "x", value, t0, t0+1))
		history = append(history, r(2, "x", value, t0+2, t0+3))
		t0 += 4
	}

	// Long after every write returned, a read observes an old value.
	history = append(history, r(3, "x", "v10", t0, t0+1))

	res := Check(history)
	if res.Verdict != NotLinearizable {
		t.Fatalf("a stale read buried in 200 correct operations was missed: %s", res)
	}
}

// contains reports whether s holds sub.
func contains(s, sub string) bool {
	return len(sub) == 0 || (len(s) >= len(sub) && indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
