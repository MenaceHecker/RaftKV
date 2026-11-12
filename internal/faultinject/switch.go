// Package faultinject holds the switches that reintroduce nine known bugs for
// the mutation suite (RUNBOOK 8.6).
//
// In the default build every switch is a constant false (off.go), so each call
// site, written as `cond || faultinject.Active(faultinject.X)` or
// `cond && !faultinject.Active(faultinject.X)`, is removed by the compiler and
// the shipped binary behaves exactly as if the call sites were absent. Under the
// faultinject tag (on.go) the switches are plain booleans set before a run.
//
// The raft, statemachine and storage packages only ever call Active. Reading the
// environment lives here, in a package outside the deterministic core, so the
// core keeps importing nothing nondeterministic.
package faultinject

// Switch names one injected bug.
type Switch int

// The nine bugs of RUNBOOK 8.6, in the order of that list.
const (
	// SkipFsync skips the fsync that makes a WAL write durable before it is
	// acknowledged (RUNBOOK 8.6 item 1). It covers entries and hard state.
	SkipFsync Switch = iota
	// TruncateOnStaleAppend truncates the log on a stale or duplicated
	// AppendEntries that carries no conflict (item 2).
	TruncateOnStaleAppend
	// CommitPreviousTerm lets a leader commit an earlier term's entry by
	// counting replicas (item 3, Raft paper section 5.4.2, Figure 8).
	CommitPreviousTerm
	// SkipVoteUpToDate grants votes and pre-votes without the up-to-date log
	// check (item 4, paper section 5.4.1).
	SkipVoteUpToDate
	// LocalRead lets a leader answer a read from local state without the
	// heartbeat quorum round (item 5, paper section 8).
	LocalRead
	// SkipDedupe applies every client command even when its session says it was
	// already applied (item 6).
	SkipDedupe
	// SkipNoop makes a new leader skip the no-op entry of its own term
	// (item 7, paper section 8).
	SkipNoop
	// SnapshotOmitsSessions writes a snapshot with an empty session table
	// (item 8).
	SnapshotOmitsSessions
	// ApplyBeyondCommit applies entries that are not yet committed (item 9).
	ApplyBeyondCommit

	count
)

var names = [count]string{
	SkipFsync:             "skip-fsync",
	TruncateOnStaleAppend: "truncate-on-stale-append",
	CommitPreviousTerm:    "commit-previous-term",
	SkipVoteUpToDate:      "skip-vote-uptodate",
	LocalRead:             "local-read",
	SkipDedupe:            "skip-dedupe",
	SkipNoop:              "skip-noop",
	SnapshotOmitsSessions: "snapshot-omits-sessions",
	ApplyBeyondCommit:     "apply-beyond-commit",
}

// Name returns the switch's name as used in RAFTKV_FAULTINJECT and in
// docs/CHAOS_RESULTS.md.
func (s Switch) Name() string {
	if s < 0 || s >= count {
		return "unknown"
	}
	return names[s]
}

// All returns the nine switches in RUNBOOK order.
func All() []Switch {
	out := make([]Switch, 0, count)
	for s := Switch(0); s < count; s++ {
		out = append(out, s)
	}
	return out
}

// Lookup finds a switch by name.
func Lookup(name string) (Switch, bool) {
	for s, n := range names {
		if n == name {
			return Switch(s), true
		}
	}
	return 0, false
}
