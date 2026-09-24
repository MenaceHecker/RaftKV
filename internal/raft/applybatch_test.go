package raft

import "testing"

func TestNextCommittedStopsAtTheCap(t *testing.T) {
	st := NewMemoryStorage()
	entries := make([]Entry, 100)
	for i := range entries {
		entries[i] = Entry{Term: 1, Index: Index(i + 1), Data: []byte("x")}
	}
	if err := st.Append(entries); err != nil {
		t.Fatalf("append: %v", err)
	}

	l := newRaftLog(st)
	l.committed = 100

	got, err := l.nextCommitted(10)
	if err != nil {
		t.Fatalf("nextCommitted: %v", err)
	}
	if len(got) != 10 {
		t.Fatalf("got %d entries, want 10", len(got))
	}
	if got[0].Index != 1 || got[9].Index != 10 {
		t.Errorf("got indexes %d..%d, want 1..10", got[0].Index, got[9].Index)
	}
}

func TestNextCommittedReturnsEverythingUnderTheCap(t *testing.T) {
	st := NewMemoryStorage()
	entries := make([]Entry, 5)
	for i := range entries {
		entries[i] = Entry{Term: 1, Index: Index(i + 1), Data: []byte("x")}
	}
	if err := st.Append(entries); err != nil {
		t.Fatalf("append: %v", err)
	}

	l := newRaftLog(st)
	l.committed = 5

	got, err := l.nextCommitted(1000)
	if err != nil {
		t.Fatalf("nextCommitted: %v", err)
	}
	if len(got) != 5 {
		t.Errorf("got %d entries, want all 5", len(got))
	}
}

func TestHasUnapplied(t *testing.T) {
	st := NewMemoryStorage()
	if err := st.Append([]Entry{{Term: 1, Index: 1, Data: []byte("x")}}); err != nil {
		t.Fatalf("append: %v", err)
	}
	l := newRaftLog(st)

	if l.hasUnapplied() {
		t.Error("a log with nothing committed reports unapplied entries")
	}
	l.committed = 1
	if !l.hasUnapplied() {
		t.Error("a log with a committed, unapplied entry reports none")
	}
	l.applied = 1
	if l.hasUnapplied() {
		t.Error("a fully applied log still reports unapplied entries")
	}
}

func TestReadyHandsBackAtMostTheCap(t *testing.T) {
	st := &countingStorage{Storage: NewMemoryStorage()}
	n, err := NewNode(Config{
		ID:                  1,
		Peers:               []NodeID{1},
		Storage:             st,
		ElectionTick:        10,
		HeartbeatTick:       1,
		MaxCommittedEntries: 7,
	})
	if err != nil {
		t.Fatalf("creating node: %v", err)
	}
	if err := n.Step(Message{Type: MsgCampaign}); err != nil {
		t.Fatalf("campaign: %v", err)
	}

	datas := make([][]byte, 30)
	for i := range datas {
		datas[i] = []byte("cmd")
	}
	if err := n.ProposeBatch(datas); err != nil {
		t.Fatalf("propose: %v", err)
	}

	var batches, total int
	for {
		rd := n.Ready()
		if len(rd.CommittedEntries) == 0 {
			break
		}
		if len(rd.CommittedEntries) > 7 {
			t.Fatalf("a Ready handed back %d entries, over the cap of 7",
				len(rd.CommittedEntries))
		}
		batches++
		total += len(rd.CommittedEntries)
		n.Advance(rd)

		if batches > 20 {
			t.Fatal("Ready never drained; the applied cursor is not advancing")
		}
	}

	if batches < 4 {
		t.Errorf("the backlog came back in %d batches; the cap is not being exercised", batches)
	}
	if total < 30 {
		t.Errorf("only %d entries were handed back in total, want at least 30", total)
	}
}
