// Package client is a leader-following client for the RaftKV KVService.
//
// It rotates across a static node list, follows NotLeader hints, backs off
// with bounded jittered exponential delay, and retries a write with the same
// (client id, sequence) pair so that a retry is applied at most once (Raft
// paper section 8: clients find the leader, retry, and tag every command with
// a unique serial number so the state machine can drop duplicates).
//
// # Outcomes
//
// A read either returns a value or an error that never means "unknown". A
// write ends in one of three ways:
//
//   - nil: it was committed (or recognised as an already-applied duplicate).
//   - *DefinitiveError: it cannot have been applied.
//   - *IndeterminateError: at least one attempt was sent and no definite answer
//     came back. It may or may not be applied. Every NotLeader or
//     lost-leadership reply to a write counts as indeterminate, because the
//     server can send them after the entry was proposed. Test with
//     IsIndeterminate.
//
// # Client ids, sequence numbers and the restart hazard
//
// The server keeps one session per client id and drops any write whose
// sequence is not above the highest it has applied for that id, while still
// reporting success. Two consequences:
//
//   - A Client serialises its writes with an internal mutex, so one client id
//     never has two writes in flight (two writes applied in the opposite order
//     would lose the lower one while the caller saw success). Use several
//     Clients for parallel writes; each gets its own id.
//   - If Config.ClientID is zero the client draws a random non-zero id from
//     Config.Rand, which has no session on the server, so sequence 1 is safe.
//     A caller that sets ClientID itself also owns StartSeq. Reusing a fixed id
//     across a process restart with the sequence starting again at 1 makes the
//     server silently drop every write until the sequence passes the old
//     high-water mark, yet each one is reported as success. The server has no
//     session expiry and no way to read the mark, so the client cannot detect
//     this.
//
// A smaller hazard in the other direction: the server keeps at most 4096
// sessions and evicts the least recently used when a new id arrives. If a
// session is evicted between an attempt and its retry, the retry is applied a
// second time. Every restart that draws a fresh random id adds one session.
//
// # Time and randomness
//
// The retry logic uses only the injected Clock and Rand, never time.Now,
// time.Sleep or the math/rand global.
package client
