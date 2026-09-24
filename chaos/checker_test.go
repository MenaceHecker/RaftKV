package chaos

import (
	"fmt"
	"testing"
)

func w(client int, key, value string, invoked, returned int64) Op {
	return Op{
		Kind: OpWrite, Client: client, Key: key, Value: value,
		Invoked: invoked, Returned: returned, Status: StatusOK,
	}
}

func r(client int, key, value string, invoked, returned int64) Op {
	return Op{
		Kind: OpRead, Client: client, Key: key, Value: value, Found: true,
		Invoked: invoked, Returned: returned, Status: StatusOK,
	}
}

func rAbsent(client int, key string, invoked, returned int64) Op {
	return Op{
		Kind: OpRead, Client: client, Key: key, Found: false,
		Invoked: invoked, Returned: returned, Status: StatusOK,
	}
}

func unknown(op Op) Op {
	op.Status = StatusUnknown
	return op
}

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
	assertNotLinearizable(t, []Op{
		w(1, "x", "a", 0, 1),
		w(1, "x", "b", 2, 3),
		r(2, "x", "a", 4, 5),
	})
}

func TestReadingAValueFromTheFutureIsRejected(t *testing.T) {
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
	assertNotLinearizable(t, []Op{
		w(1, "x", "a", 0, 1),
		rAbsent(2, "x", 2, 3),
	})
}

func TestGoingBackwardsIsRejected(t *testing.T) {
	assertNotLinearizable(t, []Op{
		w(1, "x", "a", 0, 1),
		w(1, "x", "b", 2, 3),
		r(2, "x", "b", 4, 5),
		r(2, "x", "a", 6, 7),
	})
}

func TestConcurrentWritesMayBeOrderedEitherWay(t *testing.T) {
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
	assertNotLinearizable(t, []Op{
		w(1, "x", "a", 0, 1),
		w(1, "x", "b", 2, 3),
		r(2, "x", "b", 4, 5),
		r(3, "x", "a", 6, 7),
	})
}

func TestFailedOperationsAreIgnored(t *testing.T) {
	assertLinearizable(t, []Op{
		w(1, "x", "a", 0, 1),
		failed(w(2, "x", "b", 2, 3)),
		r(3, "x", "a", 4, 5),
	})
}

func TestFailedWritesCannotExplainARead(t *testing.T) {
	assertNotLinearizable(t, []Op{
		failed(w(1, "x", "a", 0, 1)),
		r(2, "x", "a", 2, 3),
	})
}

func TestUnknownWritesMayOrMayNotHaveHappened(t *testing.T) {

	assertLinearizable(t, []Op{
		unknown(w(1, "x", "a", 0, 5)),
		r(2, "x", "a", 6, 7),
	})

	assertLinearizable(t, []Op{
		unknown(w(1, "x", "a", 0, 5)),
		rAbsent(2, "x", 6, 7),
	})
}

func TestUnknownWritesCannotExcuseEverything(t *testing.T) {
	assertNotLinearizable(t, []Op{
		unknown(w(1, "x", "a", 0, 5)),
		r(2, "x", "z", 6, 7),
	})
}

func TestAnUnknownWriteCannotMoveBeforeItsInvocation(t *testing.T) {
	assertNotLinearizable(t, []Op{
		r(1, "x", "a", 0, 1),
		unknown(w(2, "x", "a", 2, 9)),
	})
}

func TestKeysAreCheckedIndependently(t *testing.T) {
	assertLinearizable(t, []Op{
		w(1, "x", "a", 0, 1),
		w(1, "y", "b", 0, 1),
		r(2, "x", "a", 2, 3),
		r(2, "y", "b", 2, 3),
	})

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
	var history []Op
	for i := range 12 {
		history = append(history, w(1, "x", fmt.Sprintf("v%d", i), 0, 1000))
	}
	history = append(history, r(2, "x", "never-written", 1001, 1002))

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
	var history []Op
	var t0 int64
	for i := range 100 {
		value := fmt.Sprintf("v%d", i)
		history = append(history, w(1, "x", value, t0, t0+1))
		history = append(history, r(2, "x", value, t0+2, t0+3))
		t0 += 4
	}

	history = append(history, r(3, "x", "v10", t0, t0+1))

	res := Check(history)
	if res.Verdict != NotLinearizable {
		t.Fatalf("a stale read buried in 200 correct operations was missed: %s", res)
	}
}

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
