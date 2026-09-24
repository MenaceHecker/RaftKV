package chaos

import (
	"testing"

	"github.com/MenaceHecker/raftkv/internal/raft"
)

func msg(from, to raft.NodeID) raft.Message {
	return raft.Message{Type: raft.MsgHeartbeat, From: from, To: to, Term: 1}
}

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
		msg(1, 2),
		msg(3, 4),
		msg(1, 3),
		msg(4, 2),
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
	n, err := NewNetwork(1, Faults{})
	if err != nil {
		t.Fatalf("NewNetwork: %v", err)
	}

	n.Partition([]raft.NodeID{1, 2, 3})

	n.Send([]raft.Message{
		msg(1, 2),
		msg(1, 4),
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
	n, err := NewNetwork(3, Faults{MinDelay: 0, MaxDelay: 8})
	if err != nil {
		t.Fatalf("NewNetwork: %v", err)
	}

	const count = 60
	for i := range count {
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
	n, err := NewNetwork(9, Faults{LossRate: 0.3, MinDelay: 0, MaxDelay: 4})
	if err != nil {
		t.Fatalf("NewNetwork: %v", err)
	}
	n.Partition([]raft.NodeID{1, 2}, []raft.NodeID{3})

	const count = 300
	for i := range count {
		to := raft.NodeID(2)
		if i%3 == 0 {
			to = 3
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

func observedRate(t *testing.T, seed int64, f Faults, n int, count func(Stats) int) float64 {
	t.Helper()

	net, err := NewNetwork(seed, f)
	if err != nil {
		t.Fatalf("creating the network: %v", err)
	}
	for i := range n {
		net.Send([]raft.Message{{From: 1, To: 2, Term: raft.Term(i)}})
	}
	return float64(count(net.Stats())) / float64(n)
}

func assertClose(t *testing.T, what string, want, got, tolerance float64) {
	t.Helper()

	drift := got - want
	if drift < 0 {
		drift = -drift
	}
	if drift > want*tolerance {
		t.Errorf("%s configured at %.3f but observed at %.4f, which is %.0f%% out",
			what, want, got, drift/want*100)
	}
}

func TestLossRateMatchesItsConfiguration(t *testing.T) {
	const (
		messages  = 20000
		tolerance = 0.10
	)
	for _, want := range []float64{0.05, 0.15, 0.30, 0.50} {
		got := observedRate(t, 1, Faults{LossRate: want}, messages,
			func(s Stats) int { return s.Dropped })
		assertClose(t, "loss", want, got, tolerance)
	}
}

func TestDuplicateRateMatchesItsConfiguration(t *testing.T) {
	const (
		messages  = 20000
		tolerance = 0.10
	)
	for _, want := range []float64{0.10, 0.25, 0.50} {
		got := observedRate(t, 2, Faults{DuplicateRate: want}, messages,
			func(s Stats) int { return s.Duplicated })
		assertClose(t, "duplication", want, got, tolerance)
	}
}

func TestDelaysStayWithinTheirBoundsAndReachThem(t *testing.T) {
	const (
		min      = 2
		max      = 6
		messages = 2000
	)

	net, err := NewNetwork(3, Faults{MinDelay: min, MaxDelay: max})
	if err != nil {
		t.Fatalf("creating the network: %v", err)
	}
	for i := range messages {
		net.Send([]raft.Message{{From: 1, To: 2, Term: raft.Term(i)}})
	}

	seen := map[int64]int{}
	for _, q := range net.queue {
		d := q.at - net.Now()
		if d < min || d > max {
			t.Fatalf("a message was delayed %d ticks, outside the configured [%d, %d]", d, min, max)
		}
		seen[d]++
	}
	for d := int64(min); d <= max; d++ {
		if seen[d] == 0 {
			t.Errorf("no message was delayed by %d ticks, so that end of [%d, %d] is unreachable",
				d, min, max)
		}
	}
}
