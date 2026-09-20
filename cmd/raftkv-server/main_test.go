package main

import (
	"strings"
	"testing"

	"github.com/MenaceHecker/raftkv/internal/raft"
)

// Tests for the entrypoint's handling of what an operator types.
//
// This is the only code in the system whose input is a human under time
// pressure, and it had no tests. Misconfiguration is not an exotic failure
// here: the peer list is how every node learns who the cluster is, and the
// ways it can be wrong mostly fail late and quietly rather than at startup.
// The parser's job is to turn those into a refusal with a reason.

func TestParsePeersAcceptsAWellFormedList(t *testing.T) {
	peers, err := parsePeers("1=127.0.0.1:9001,2=127.0.0.1:9002,3=127.0.0.1:9003")
	if err != nil {
		t.Fatalf("a well-formed list was rejected: %v", err)
	}
	if len(peers) != 3 {
		t.Fatalf("parsed %d peers, want 3", len(peers))
	}
	if got := peers[raft.NodeID(2)]; got != "127.0.0.1:9002" {
		t.Errorf("peer 2 is at %q, want 127.0.0.1:9002", got)
	}
}

func TestParsePeersToleratesWhitespaceAndStrayCommas(t *testing.T) {
	// An operator pasting a list across lines should not have to think about
	// spacing.
	peers, err := parsePeers(" 1 = host-a:9001 , 2=host-b:9002 , ")
	if err != nil {
		t.Fatalf("a spaced list was rejected: %v", err)
	}
	if len(peers) != 2 {
		t.Fatalf("parsed %d peers, want 2", len(peers))
	}
	if got := peers[raft.NodeID(1)]; got != "host-a:9001" {
		t.Errorf("peer 1 is at %q; whitespace was not trimmed", got)
	}
}

func TestParsePeersRejectsMalformedLists(t *testing.T) {
	// Each of these has a specific reason, and the message has to name it:
	// an operator reading it at three in the morning should not have to
	// diff the string by eye.
	for _, tc := range []struct {
		name  string
		spec  string
		wants string
	}{
		{"empty", "", "required"},
		{"only whitespace", "   ", "required"},
		{"only commas", ",,,", "no members"},
		{"no equals sign", "1:127.0.0.1:9001", "id=host:port"},
		{"unreadable id", "abc=127.0.0.1:9001", "unreadable ID"},
		{"reserved id zero", "0=127.0.0.1:9001", "reserved ID 0"},
		{"missing address", "1=", "no address"},
		{"whitespace address", "1=   ", "no address"},
		{"repeated id", "1=host-a:9001,1=host-b:9002", "more than once"},
		{"repeated address", "1=host-a:9001,2=host-a:9001", "both at"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parsePeers(tc.spec)
			if err == nil {
				t.Fatalf("parsePeers(%q) was accepted", tc.spec)
			}
			if !strings.Contains(err.Error(), tc.wants) {
				t.Errorf("parsePeers(%q) said %q, which does not mention %q",
					tc.spec, err, tc.wants)
			}
		})
	}
}

func TestParsePeersNamesBothSidesOfAnAddressCollision(t *testing.T) {
	// The whole value of catching this is being told which two lines to
	// look at.
	_, err := parsePeers("1=host-a:9001,2=host-b:9002,3=host-a:9001")
	if err == nil {
		t.Fatal("two peers sharing an address were accepted")
	}
	for _, want := range []string{"1", "3", "host-a:9001"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the error %q does not mention %q", err, want)
		}
	}
}

func TestSortedIDsAreAscendingRegardlessOfInputOrder(t *testing.T) {
	// Every node derives its initial configuration from this order, so two
	// nodes given the same members in a different order must still agree.
	a, err := parsePeers("3=c:3,1=a:1,2=b:2")
	if err != nil {
		t.Fatalf("parsing: %v", err)
	}
	b, err := parsePeers("2=b:2,3=c:3,1=a:1")
	if err != nil {
		t.Fatalf("parsing: %v", err)
	}

	first, second := sortedIDs(a), sortedIDs(b)
	if len(first) != 3 {
		t.Fatalf("got %d ids, want 3", len(first))
	}
	for i := range first {
		if first[i] != second[i] {
			t.Fatalf("the same members in a different order produced %v and %v", first, second)
		}
	}
	for i := 1; i < len(first); i++ {
		if first[i] <= first[i-1] {
			t.Errorf("ids are not ascending: %v", first)
		}
	}
}

func TestNewLoggerRejectsAnUnknownLevel(t *testing.T) {
	for _, level := range []string{"debug", "info", "warn", "error"} {
		if _, err := newLogger(level); err != nil {
			t.Errorf("newLogger(%q) failed: %v", level, err)
		}
	}
	if _, err := newLogger("chatty"); err == nil {
		t.Error("an unknown log level was accepted")
	}
}
