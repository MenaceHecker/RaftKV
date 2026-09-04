package chaos

import (
	"testing"

	"github.com/MenaceHecker/raftkv/internal/raft"
)

// Tests for the fault-injecting network.
//
// This is test infrastructure, which is exactly why it needs tests of its own.
// A chaos harness that silently fails to inject the faults it claims turns
// every scenario built on it into a test of nothing, and the failure mode is
// invisible: everything passes, and it looks like the system is robust.

// msg builds a message between two nodes.
func msg(from, to raft.NodeID) raft.Message {
	return raft.Message{Type: raft.MsgHeartbeat, From: from, To: to, Term: 1}
}

// drainAt advances to a tick and returns everything delivered up to it.
func drainAt(n *Network, ticks int) []raft.Message {
	var out []raft.Message
	for range ticks {
		out = append(out, n.Deliver()...)
		n.Advance()
	}
	return append(out, n.Deliver()...)
}

func TestPerfectNetworkDeliversEverythingImmediately(t *testing.T) {
	n, err := NewNetwork(1, Faults{})
	if err != nil {
		t.Fatalf("NewNetwork: %v", err)
	}

	n.Send([]raft.Message{msg(1, 2), msg(1, 3), msg(2, 1)})

	got := n.Deliver()
	if len(got) != 3 {
		t.Fatalf("delivered %d messages, want 3", len(got))
	}
	if n.InFlight() != 0 {
		t.Fatalf("%d messages still in flight on a perfect network", n.InFlight())
	}
}

func TestDeliveryOrderIsDeterministic(t *testing.T) {
	// The whole harness rests on this. If the same seed produced a different
	// delivery order between runs, a failing scenario could not be reproduced
	// and the suite would only teach people to re-run it.
	run := func() []raft.Message {
		n, err := NewNetwork(42, Faults{MinDelay: 0, MaxDelay: 5})
		if err != nil {
			t.Fatalf("NewNetwork: %v", err)
		}
		for i := range 50 {
			n.Send([]raft.Message{msg(raft.NodeID(i%3+1), raft.NodeID(i%3+2))})
		}
		return drainAt(n, 10)
	}

	first := run()
	for i := range 20 {
		got := run()
		if len(got) != len(first) {
			t.Fatalf("run %d delivered %d messages, first run delivered %d",
				i, len(got), len(first))
		}
		for j := range first {
			if got[j].From != first[j].From || got[j].To != first[j].To {
				t.Fatalf("run %d differs at position %d: %d->%d, want %d->%d",
					i, j, got[j].From, got[j].To, first[j].From, first[j].To)
			}
		}
	}
}

func TestDifferentSeedsExploreDifferentOrders(t *testing.T) {
	// Reproducibility is only useful if the seed actually changes something.
	// A harness where every seed behaved identically would explore one
	// interleaving forever while appearing to explore many.
	order := func(seed int64) []raft.NodeID {
		n, err := NewNetwork(seed, Faults{MinDelay: 0, MaxDelay: 10})
		if err != nil {
			t.Fatalf("NewNetwork: %v", err)
		}
		for i := range 40 {
			n.Send([]raft.Message{msg(1, raft.NodeID(i%5+2))})
		}
		var out []raft.NodeID
		for _, m := range drainAt(n, 15) {
			out = append(out, m.To)
		}
		return out
	}

	a, b := order(1), order(2)
	same := len(a) == len(b)
	if same {
		for i := range a {
			if a[i] != b[i] {
				same = false
				break
			}
		}
	}
	if same {
		t.Fatal("two different seeds produced identical delivery orders; the seed " +
			"is not actually driving the schedule")
	}
}

func TestPartitionDropsCrossingMessages(t *testing.T) {
	n, err := NewNetwork(1, Faults{})
	if err != nil {
		t.Fatalf("NewNetwork: %v", err)
	}

	n.Partition([]raft.NodeID{1, 2}, []raft.NodeID{3, 4})

	n.Send([]raft.Message{
		msg(1, 2), // within a group
		msg(3, 4), // within a group
		msg(1, 3), // crossing
		msg(4, 2), // crossing
	})

	got := n.Deliver()
	if len(got) != 2 {
		t.Fatalf("delivered %d messages across a partition, want 2", len(got))
	}
	for _, m := range got {
		if (m.From <= 2) != (m.To <= 2) {
			t.Fatalf("a message crossed the partition: %d->%d", m.From, m.To)
		}
	}
	if n.Stats().Partitions != 2 {
		t.Fatalf("counted %d partitioned messages, want 2", n.Stats().Partitions)
	}
}

func TestUnlistedNodesAreIsolated(t *testing.T) {
	// Naming one group has to be enough to isolate a node, or every scenario
	// would have to enumerate the whole cluster just to cut one member off.
	n, err := NewNetwork(1, Faults{})
	if err != nil {
		t.Fatalf("NewNetwork: %v", err)
	}

	n.Partition([]raft.NodeID{1, 2, 3})

	n.Send([]raft.Message{
		msg(1, 2), // both listed
		msg(1, 4), // 4 is not listed
		msg(4, 1),
	})

	got := n.Deliver()
	if len(got) != 1 {
		t.Fatalf("delivered %d messages, want only the one between listed nodes", len(got))
	}
	if got[0].From != 1 || got[0].To != 2 {
		t.Fatalf("delivered %d->%d, want 1->2", got[0].From, got[0].To)
	}
}

func TestHealingRestoresConnectivity(t *testing.T) {
	n, err := NewNetwork(1, Faults{})
	if err != nil {
		t.Fatalf("NewNetwork: %v", err)
	}

	n.Partition([]raft.NodeID{1}, []raft.NodeID{2})
	n.Send([]raft.Message{msg(1, 2)})
	if got := len(n.Deliver()); got != 0 {
		t.Fatalf("delivered %d messages across a partition", got)
	}

	n.Heal()
	n.Send([]raft.Message{msg(1, 2)})
	if got := len(n.Deliver()); got != 1 {
		t.Fatalf("delivered %d messages after healing, want 1", got)
	}
}

func TestHealingDoesNotResurrectDroppedMessages(t *testing.T) {
	// A message a partition dropped is gone. Delivering it once the partition
	// heals would model a network that buffers indefinitely, which is both
	// unrealistic and far easier to survive than the real thing — Raft's own
	// retransmission is what recovers, and that is what should be under test.
	n, err := NewNetwork(1, Faults{})
	if err != nil {
		t.Fatalf("NewNetwork: %v", err)
	}

	n.Partition([]raft.NodeID{1}, []raft.NodeID{2})
	n.Send([]raft.Message{msg(1, 2)})
	n.Heal()

	if got := len(n.Deliver()); got != 0 {
		t.Fatalf("a message dropped by a partition was delivered after healing")
	}
	if n.InFlight() != 0 {
		t.Fatalf("%d dropped messages are still queued", n.InFlight())
	}
}

func TestLossDropsSomeButNotAll(t *testing.T) {
	n, err := NewNetwork(7, Faults{LossRate: 0.5})
	if err != nil {
		t.Fatalf("NewNetwork: %v", err)
	}

	const count = 1000
	for range count {
		n.Send([]raft.Message{msg(1, 2)})
	}
	delivered := len(n.Deliver())

	st := n.Stats()
	if st.Dropped == 0 {
		t.Fatal("a 50% loss rate dropped nothing")
	}
	if delivered == 0 {
		t.Fatal("a 50% loss rate dropped everything")
	}
	if st.Sent != count {
		t.Fatalf("counted %d sent, want %d", st.Sent, count)
	}
	if st.Dropped+delivered != count {
		t.Fatalf("%d dropped plus %d delivered does not account for %d sent",
			st.Dropped, delivered, count)
	}

	// Loosely around half. The bound is wide because this checks that the rate
	// is applied at all, not that the generator is well distributed.
	if st.Dropped < count/5 || st.Dropped > 4*count/5 {
		t.Fatalf("dropped %d of %d at a 50%% loss rate, which is not close to half",
			st.Dropped, count)
	}
}

func TestZeroLossDropsNothing(t *testing.T) {
	n, err := NewNetwork(1, Faults{})
	if err != nil {
		t.Fatalf("NewNetwork: %v", err)
	}
	for range 500 {
		n.Send([]raft.Message{msg(1, 2)})
	}
	if got := len(n.Deliver()); got != 500 {
		t.Fatalf("delivered %d of 500 on a lossless network", got)
	}
}

func TestDelayHoldsMessagesBack(t *testing.T) {
	n, err := NewNetwork(1, Faults{MinDelay: 3, MaxDelay: 3})
	if err != nil {
		t.Fatalf("NewNetwork: %v", err)
	}

	n.Send([]raft.Message{msg(1, 2)})

	for tick := range 3 {
		if got := len(n.Deliver()); got != 0 {
			t.Fatalf("a message with a 3-tick delay arrived at tick %d", tick)
		}
		n.Advance()
	}
	if got := len(n.Deliver()); got != 1 {
		t.Fatalf("delivered %d messages at the delay boundary, want 1", got)
	}
}

func TestVariableDelayReordersMessages(t *testing.T) {
	// Reordering is the fault that finds the subtlest bugs, because an
	// implementation that treats a response as being about its most recent
	// request looks perfectly correct until two cross.
	n, err := NewNetwork(3, Faults{MinDelay: 0, MaxDelay: 8})
	if err != nil {
		t.Fatalf("NewNetwork: %v", err)
	}

	const count = 60
	for i := range count {
		// The term stands in for send order, so the delivered sequence can be
		// compared against it.
		n.Send([]raft.Message{{
			Type: raft.MsgHeartbeat, From: 1, To: 2, Term: raft.Term(i),
		}})
	}

	delivered := drainAt(n, 12)
	if len(delivered) != count {
		t.Fatalf("delivered %d of %d messages", len(delivered), count)
	}

	reordered := false
	for i := 1; i < len(delivered); i++ {
		if delivered[i].Term < delivered[i-1].Term {
			reordered = true
			break
		}
	}
	if !reordered {
		t.Fatal("variable delay produced no reordering, so no scenario built on " +
			"it is testing reordered delivery")
	}
}

func TestDuplicationDeliversTwice(t *testing.T) {
	n, err := NewNetwork(5, Faults{DuplicateRate: 1.0})
	if err != nil {
		t.Fatalf("NewNetwork: %v", err)
	}

	n.Send([]raft.Message{msg(1, 2)})

	if got := len(n.Deliver()); got != 2 {
		t.Fatalf("delivered %d copies at a 100%% duplicate rate, want 2", got)
	}
	if n.Stats().Duplicated != 1 {
		t.Fatalf("counted %d duplications, want 1", n.Stats().Duplicated)
	}
}

func TestDuplicatesCanArriveLater(t *testing.T) {
	// A copy delivered back to back is a much weaker test than one that turns
	// up after the receiver has moved on, so duplicates are scheduled
	// independently rather than beside the original.
	n, err := NewNetwork(11, Faults{DuplicateRate: 1.0, MinDelay: 0, MaxDelay: 6})
	if err != nil {
		t.Fatalf("NewNetwork: %v", err)
	}

	for range 40 {
		n.Send([]raft.Message{msg(1, 2)})
	}

	first := len(n.Deliver())
	later := len(drainAt(n, 10))

	if first == 0 || later == 0 {
		t.Fatalf("%d copies arrived immediately and %d later; duplicates are not "+
			"being scheduled independently", first, later)
	}
}

func TestCrashDropsMessagesBoundForANode(t *testing.T) {
	// When a node dies, whatever was on its way to it is gone. Delivering it
	// to the restarted process would hand a node messages from before it
	// existed.
	n, err := NewNetwork(1, Faults{MinDelay: 5, MaxDelay: 5})
	if err != nil {
		t.Fatalf("NewNetwork: %v", err)
	}

	n.Send([]raft.Message{msg(1, 2), msg(1, 3), msg(2, 3)})
	if n.InFlight() != 3 {
		t.Fatalf("%d messages in flight, want 3", n.InFlight())
	}

	n.DropAllInFlight(3)
	if n.InFlight() != 1 {
		t.Fatalf("%d messages in flight after crashing node 3, want 1", n.InFlight())
	}

	got := drainAt(n, 6)
	if len(got) != 1 || got[0].To != 2 {
		t.Fatalf("delivered %d messages, want only the one bound for node 2", len(got))
	}
}

func TestFaultsCanChangeMidRun(t *testing.T) {
	// A scenario models a network that degrades and then recovers, so the
	// configuration has to be changeable without rebuilding the network and
	// losing everything in flight.
	n, err := NewNetwork(1, Faults{})
	if err != nil {
		t.Fatalf("NewNetwork: %v", err)
	}

	n.Send([]raft.Message{msg(1, 2)})
	if got := len(n.Deliver()); got != 1 {
		t.Fatalf("delivered %d on a healthy network", got)
	}

	if err := n.SetFaults(Faults{LossRate: 1.0}); err != nil {
		t.Fatalf("SetFaults: %v", err)
	}
	n.Send([]raft.Message{msg(1, 2)})
	if got := len(n.Deliver()); got != 0 {
		t.Fatalf("delivered %d at a 100%% loss rate", got)
	}

	if err := n.SetFaults(Faults{}); err != nil {
		t.Fatalf("SetFaults: %v", err)
	}
	n.Send([]raft.Message{msg(1, 2)})
	if got := len(n.Deliver()); got != 1 {
		t.Fatalf("delivered %d after recovering", got)
	}
}

func TestInvalidFaultsAreRejected(t *testing.T) {
	// A misconfigured harness that ran anyway would inject something other
	// than what the scenario asked for, and the scenario would still pass.
	cases := map[string]Faults{
		"negative loss":   {LossRate: -0.1},
		"loss above one":  {LossRate: 1.5},
		"negative dupes":  {DuplicateRate: -1},
		"dupes above one": {DuplicateRate: 2},
		"negative delay":  {MinDelay: -1},
		"max below min":   {MinDelay: 5, MaxDelay: 2},
	}

	for name, f := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := NewNetwork(1, f); err == nil {
				t.Fatalf("%s was accepted", name)
			}
			n, err := NewNetwork(1, Faults{})
			if err != nil {
				t.Fatalf("NewNetwork: %v", err)
			}
			if err := n.SetFaults(f); err == nil {
				t.Fatalf("%s was accepted by SetFaults", name)
			}
		})
	}
}

func TestStatsAccountForEveryMessage(t *testing.T) {
	// A scenario reports the conditions it actually ran under, not the ones it
	// configured. A run that asked for 20% loss and happened to drop nothing
	// proves much less, and only the counters can tell the difference.
	n, err := NewNetwork(9, Faults{LossRate: 0.3, MinDelay: 0, MaxDelay: 4})
	if err != nil {
		t.Fatalf("NewNetwork: %v", err)
	}
	n.Partition([]raft.NodeID{1, 2}, []raft.NodeID{3})

	const count = 300
	for i := range count {
		to := raft.NodeID(2)
		if i%3 == 0 {
			to = 3 // crosses the partition
		}
		n.Send([]raft.Message{msg(1, to)})
	}

	delivered := len(drainAt(n, 8))
	st := n.Stats()

	if st.Sent != count {
		t.Fatalf("counted %d sent, want %d", st.Sent, count)
	}
	if st.Partitions == 0 {
		t.Fatal("no messages were counted as partitioned")
	}
	if st.Dropped == 0 {
		t.Fatal("no messages were counted as dropped")
	}
	if st.Delivered != delivered {
		t.Fatalf("stats report %d delivered, the caller received %d", st.Delivered, delivered)
	}
	if st.Sent != st.Delivered+st.Dropped+st.Partitions-st.Duplicated {
		t.Fatalf("messages are unaccounted for: sent=%d delivered=%d dropped=%d "+
			"partitioned=%d duplicated=%d",
			st.Sent, st.Delivered, st.Dropped, st.Partitions, st.Duplicated)
	}
}
