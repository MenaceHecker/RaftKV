package storage_test

// Shared pieces of the crash-recovery tests: a rig that drives a DiskStorage
// on faultfs while recording what the caller was told, and checkRecovered,
// which holds the recovered state to RUNBOOK 7.2 item 1 and 7.3. The tests live
// in package storage_test because faultfs imports storage.

import (
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"reflect"
	"strings"
	"testing"

	"github.com/MenaceHecker/raftkv/internal/raft"
	"github.com/MenaceHecker/raftkv/internal/statemachine"
	"github.com/MenaceHecker/raftkv/internal/storage"
	"github.com/MenaceHecker/raftkv/internal/storage/faultfs"
)

const (
	dataDir = "/data"
	walDir  = "/data/wal"
	snapDir = "/data/snap"
)

var testConf = raft.ConfState{Voters: []raft.NodeID{1, 2, 3}}

func putCommand(key, value string) []byte {
	return statemachine.Command{Op: statemachine.OpPut, Key: key, Value: []byte(value)}.Encode()
}

func logEntries(term raft.Term, from raft.Index, count int) []raft.Entry {
	out := make([]raft.Entry, count)
	for i := range out {
		idx := from + raft.Index(i)
		out[i] = raft.Entry{
			Term:  term,
			Index: idx,
			Type:  raft.EntryNormal,
			Data:  putCommand(fmt.Sprintf("key-%d", idx), fmt.Sprintf("value-%d@%d", idx, term)),
		}
	}
	return out
}

// quietLogs silences the repair warnings Open logs, which thousands of crash
// points would otherwise print, and restores the logger afterwards.
func quietLogs(t *testing.T) {
	t.Helper()
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })
}

// newFS returns a faultfs whose data, wal and snap directories already exist
// durably. First-start directory loss is T3.13's finding, not this suite's.
func newFS(t *testing.T, seed int64, opts faultfs.Options) *faultfs.FS {
	t.Helper()
	// A seeded generator is the point: the same seed must give the same crash.
	f := faultfs.New(rand.New(rand.NewSource(seed)), opts) //nolint:gosec // deterministic simulation, not security
	for _, d := range []string{walDir, snapDir} {
		if err := f.MkdirAll(d, 0o755); err != nil {
			t.Fatalf("MkdirAll(%s): %v", d, err)
		}
	}
	for _, d := range []string{dataDir, "/"} {
		if err := f.SyncDir(d); err != nil {
			t.Fatalf("SyncDir(%s): %v", d, err)
		}
	}
	return f
}

// snapRec is one snapshot the caller asked for.
type snapRec struct {
	term  raft.Term
	data  []byte
	acked bool
}

// model is what the caller was told. appends holds every Append attempted, in
// order; the first appendsAcked of them returned nil. Hard states and snapshots
// are tracked the same way. A call that failed (or was cut off by a crash) is
// in flight: it may or may not have reached the disk.
type model struct {
	appends      [][]raft.Entry
	appendsAcked int

	hardStates []raft.HardState // [0] is the state before any call
	hsAcked    int

	snaps     map[raft.Index]*snapRec
	snapAcked raft.Index
}

func newModel() *model {
	return &model{hardStates: []raft.HardState{{}}, snaps: map[raft.Index]*snapRec{}}
}

// rebased returns a model in which the recovered state is the acknowledged
// truth, for a second generation of work.
func rebased(s *storage.DiskStorage, snap storage.Snapshot) *model {
	m := newModel()
	hs, _ := s.InitialState()
	m.hardStates = []raft.HardState{hs}
	if snap.Meta.Index > 0 {
		m.snaps[snap.Meta.Index] = &snapRec{term: snap.Meta.Term, data: snap.Data, acked: true}
		m.snapAcked = snap.Meta.Index
	}
	if first, last := s.FirstIndex(), s.LastIndex(); last >= first {
		tail, _ := s.Entries(first, last+1)
		m.appends = [][]raft.Entry{tail}
		m.appendsAcked = 1
	}
	return m
}

// replayStates returns, for j = 0..len(flat), the log after replaying the
// first j entries the way the WAL does: an entry at or below the current last
// index first drops everything from its index on.
func (m *model) replayStates() (states [][]raft.Entry, ackedCount int) {
	var flat []raft.Entry
	for i, batch := range m.appends {
		flat = append(flat, batch...)
		if i+1 == m.appendsAcked {
			ackedCount = len(flat)
		}
	}
	var cur []raft.Entry
	states = append(states, nil)
	for _, e := range flat {
		if n := len(cur); n > 0 && e.Index <= cur[n-1].Index {
			cut := 0
			for cut < n && cur[cut].Index < e.Index {
				cut++
			}
			cur = cur[:cut]
		}
		cur = append(append([]raft.Entry(nil), cur...), e)
		states = append(states, cur)
	}
	return states, ackedCount
}

func sameEntries(a, b []raft.Entry) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Index != b[i].Index || a[i].Term != b[i].Term || !bytes.Equal(a[i].Data, b[i].Data) {
			return false
		}
	}
	return true
}

func above(es []raft.Entry, idx raft.Index) []raft.Entry {
	var out []raft.Entry
	for _, e := range es {
		if e.Index > idx {
			out = append(out, e)
		}
	}
	return out
}

func describe(es []raft.Entry) string {
	if len(es) == 0 {
		return "(none)"
	}
	var b strings.Builder
	for _, e := range es {
		fmt.Fprintf(&b, "%d/%d ", e.Index, e.Term)
	}
	return strings.TrimSpace(b.String())
}

// checkRecovered holds a reopened store to the model. policy is SyncAlways
// (every acknowledged write must be present) or SyncNever (any suffix of the
// writes, snapshots excepted, may be lost). In both, the recovered log is what
// replaying some prefix of the attempted appends gives, never more than was
// attempted and never with a hole or a reorder.
func checkRecovered(t *testing.T, ctx string, f *faultfs.FS, s *storage.DiskStorage, snap storage.Snapshot,
	m *model, policy storage.SyncPolicy) {
	t.Helper()
	fail := func(format string, args ...any) {
		t.Helper()
		t.Fatalf("%s: %s", ctx, fmt.Sprintf(format, args...))
	}

	// Snapshot: snapshots are always fsynced, under either policy.
	r := snap.Meta.Index
	if r < m.snapAcked {
		fail("recovered snapshot %d is older than the acknowledged snapshot %d", r, m.snapAcked)
	}
	var rec *snapRec
	if r > 0 {
		rec = m.snaps[r]
		if rec == nil {
			fail("recovered snapshot %d was never requested", r)
		}
		if snap.Meta.Term != rec.term {
			fail("snapshot %d has term %d, want %d", r, snap.Meta.Term, rec.term)
		}
		if !bytes.Equal(snap.Data, rec.data) {
			fail("snapshot %d data differs from what was saved", r)
		}
		if !reflect.DeepEqual(snap.Conf, testConf) {
			fail("snapshot %d configuration = %+v, want %+v", r, snap.Conf, testConf)
		}
	}
	if got := s.SnapshotMeta().Index; got != r {
		fail("store boundary is %d but the loaded snapshot is %d", got, r)
	}
	if got := s.FirstIndex(); got != r+1 {
		fail("FirstIndex = %d, want %d (snapshot index + 1)", got, r+1)
	}
	if last := s.LastIndex(); last < r {
		fail("LastIndex %d is below the snapshot index %d", last, r)
	}

	// Log: contiguous from snapshot+1, and a prefix of the attempted history.
	var tail []raft.Entry
	if first, last := s.FirstIndex(), s.LastIndex(); last >= first {
		var err error
		if tail, err = s.Entries(first, last+1); err != nil {
			fail("reading the recovered log: %v", err)
		}
	}
	for i, e := range tail {
		if want := raft.Index(i) + r + 1; e.Index != want {
			fail("recovered log has index %d at position %d, want %d (a hole or a reorder)\nlog: %s",
				e.Index, i, want, describe(tail))
		}
	}

	states, ackedCount := m.replayStates()
	lo := 0
	if policy == storage.SyncAlways {
		lo = ackedCount
	}
	matched := false
	for j := lo; j < len(states); j++ {
		if sameEntries(above(states[j], r), tail) {
			matched = true
			break
		}
	}
	if !matched {
		var acked []raft.Entry
		if ackedCount < len(states) {
			acked = above(states[ackedCount], r)
		}
		fail("recovered log is not a legal prefix (policy %s)\nrecovered:    %s\nacknowledged: %s\nfull attempt: %s",
			policy, describe(tail), describe(acked), describe(above(states[len(states)-1], r)))
	}

	// Hard state: some state the caller set, none older than the last acknowledged.
	hs, _ := s.InitialState()
	hsLo := 0
	if policy == storage.SyncAlways {
		hsLo = m.hsAcked
	}
	okHS := false
	for _, want := range m.hardStates[hsLo:] {
		if hs == want {
			okHS = true
			break
		}
	}
	if !okHS {
		fail("recovered hard state %+v is not among %+v (acknowledged index %d, policy %s)",
			hs, m.hardStates[hsLo:], m.hsAcked-hsLo, policy)
	}

	// Snapshot plus tail rebuilds a coherent state machine.
	kv := statemachine.New()
	if r > 0 {
		if err := kv.Restore(snap.Data); err != nil {
			fail("restoring snapshot %d: %v", r, err)
		}
		if kv.Applied() != r {
			fail("snapshot %d restores a state machine applied through %d", r, kv.Applied())
		}
	}
	for _, e := range tail {
		if err := kv.Apply(e); err != nil {
			fail("replaying %d on top of snapshot %d: %v", e.Index, r, err)
		}
	}
	if kv.Applied() != s.LastIndex() {
		fail("rebuilt state machine applied through %d, last index is %d", kv.Applied(), s.LastIndex())
	}

	// A partial snapshot is never left lying around once the store is open.
	des, err := f.ReadDir(snapDir)
	if err != nil {
		fail("listing %s: %v", snapDir, err)
	}
	for _, d := range des {
		if strings.HasSuffix(d.Name(), ".tmp") {
			fail("temporary snapshot %s survived open", d.Name())
		}
	}
}

// rig drives a DiskStorage on a faultfs and records, in m, what the caller was
// told. full is the caller's view of the whole log, never compacted.
type rig struct {
	t    *testing.T
	fs   *faultfs.FS
	cfg  storage.DiskConfig
	s    *storage.DiskStorage
	snap storage.Snapshot
	m    *model

	// full is the caller's view of the log from fullBase+1 on, never
	// compacted; baseData is the state machine snapshot at fullBase (nil when
	// fullBase is 0).
	full     []raft.Entry
	fullBase raft.Index
	baseData []byte

	// repairs counts OnRepair callbacks, which fire once a repair is durable.
	repairs int
}

func newRig(t *testing.T, f *faultfs.FS, policy storage.SyncPolicy, segmentSize int64) *rig {
	t.Helper()
	r := &rig{t: t, fs: f, m: newModel()}
	r.cfg = storage.DiskConfig{
		Dir:         dataDir,
		Sync:        policy,
		SegmentSize: segmentSize,
		FS:          f,
		OnRepair:    func(storage.RepairInfo) { r.repairs++ },
	}
	return r
}

func (r *rig) open() error {
	s, snap, err := storage.OpenDiskStorage(r.cfg)
	if err != nil {
		return err
	}
	r.s, r.snap = s, snap
	return nil
}

func (r *rig) append(es []raft.Entry) error {
	r.m.appends = append(r.m.appends, es)
	if err := r.s.Append(es); err != nil {
		return err
	}
	r.m.appendsAcked = len(r.m.appends)
	keep := es[0].Index - r.fullBase - 1
	r.full = append(r.full[:keep:keep], es...)
	return nil
}

func (r *rig) setHardState(hs raft.HardState) error {
	r.m.hardStates = append(r.m.hardStates, hs)
	if err := r.s.SetHardState(hs); err != nil {
		return err
	}
	r.m.hsAcked = len(r.m.hardStates) - 1
	return nil
}

// stateAt is the state machine snapshot after applying the caller's log through idx.
func (r *rig) stateAt(idx raft.Index) []byte {
	r.t.Helper()
	kv := statemachine.New()
	if r.baseData != nil {
		if err := kv.Restore(r.baseData); err != nil {
			r.t.Fatalf("restoring the base snapshot: %v", err)
		}
	}
	for _, e := range r.full[:idx-r.fullBase] {
		if err := kv.Apply(e); err != nil {
			r.t.Fatalf("applying %d: %v", e.Index, err)
		}
	}
	data, err := kv.Snapshot()
	if err != nil {
		r.t.Fatalf("snapshotting: %v", err)
	}
	return data
}

// lastIndex is the last index of the caller's view of the log.
func (r *rig) lastIndex() raft.Index { return r.fullBase + raft.Index(len(r.full)) }

// rebase makes the recovered store the starting point of the next generation.
func (r *rig) rebase() {
	r.t.Helper()
	r.m = rebased(r.s, r.snap)
	r.fullBase, r.baseData, r.full = r.snap.Meta.Index, nil, nil
	if r.fullBase > 0 {
		r.baseData = r.snap.Data
	}
	if first, last := r.s.FirstIndex(), r.s.LastIndex(); last >= first {
		tail, err := r.s.Entries(first, last+1)
		if err != nil {
			r.t.Fatalf("reading the recovered log: %v", err)
		}
		r.full = tail
	}
}

func (r *rig) createSnapshot(idx raft.Index) error {
	data := r.stateAt(idx)
	r.m.snaps[idx] = &snapRec{term: r.full[idx-r.fullBase-1].Term, data: data}
	if err := r.s.CreateSnapshot(idx, data, testConf); err != nil {
		return err
	}
	r.m.snaps[idx].acked = true
	r.m.snapAcked = idx
	return nil
}

// applySnapshot installs a snapshot of a leader log this node only partly has.
func (r *rig) applySnapshot(leader []raft.Entry, idx raft.Index) error {
	kv := statemachine.New()
	for _, e := range leader[:idx] {
		if err := kv.Apply(e); err != nil {
			r.t.Fatalf("applying %d: %v", e.Index, err)
		}
	}
	data, err := kv.Snapshot()
	if err != nil {
		r.t.Fatalf("snapshotting: %v", err)
	}
	term := leader[idx-1].Term
	r.m.snaps[idx] = &snapRec{term: term, data: data}
	if err := r.s.ApplySnapshot(raft.Snapshot{Index: idx, Term: term, Conf: testConf, Data: data}); err != nil {
		return err
	}
	r.m.snaps[idx].acked = true
	r.m.snapAcked = idx
	r.fullBase, r.baseData, r.full = idx, data, nil
	return nil
}

// reopen opens the store again on the same filesystem, after a crash, failing
// the test if Open returns an error.
func (r *rig) reopen(ctx string) {
	r.t.Helper()
	if err := r.open(); err != nil {
		r.t.Fatalf("%s: Open after the crash returned %v", ctx, err)
	}
}

func (r *rig) check(ctx string) {
	r.t.Helper()
	checkRecovered(r.t, ctx, r.fs, r.s, r.snap, r.m, r.cfg.Sync)
}

// crashMode is one way unsynced bytes can come back.
type crashMode struct {
	name string
	seed int64
	opts faultfs.CrashOptions
}

// standardModes: drop, torn x seeds 1-5, garbage x seeds 1-5, zero.
func standardModes() []crashMode {
	modes := []crashMode{{"drop", 1, faultfs.CrashOptions{}}}
	for seed := int64(1); seed <= 5; seed++ {
		modes = append(modes,
			crashMode{fmt.Sprintf("torn/seed%d", seed), seed, faultfs.CrashOptions{Tail: faultfs.TornTail}},
			crashMode{fmt.Sprintf("garbage/seed%d", seed), seed, faultfs.CrashOptions{Tail: faultfs.GarbageTail}})
	}
	return append(modes, crashMode{"zero", 1, faultfs.CrashOptions{Tail: faultfs.ZeroTail}})
}

// scenario is a deterministic workload. It returns the first error it sees,
// which after a crash point is faultfs.ErrCrashed.
type scenario struct {
	policy      storage.SyncPolicy
	segmentSize int64
	work        func(r *rig) error

	// prepare runs first, on the same filesystem, with no crash point armed.
	prepare func(r *rig)
	// inspect runs after the dry run, to confirm the workload exercises what
	// the test is about (a segment roll, a compaction).
	inspect func(t *testing.T, r *rig)
	// then runs after each crash has been checked, to go on to a second crash.
	then func(t *testing.T, r *rig, ctx string, mode crashMode)
}

// crashEverywhere runs sc once to learn N, the number of filesystem operations
// it performs, then for every n in 1..N crashes before and after operation n,
// under every mode, and holds the recovered store to the checker.
func crashEverywhere(t *testing.T, sc scenario, modes []crashMode) {
	t.Helper()
	quietLogs(t)

	dry := newRig(t, newFS(t, 1, faultfs.Options{}), sc.policy, sc.segmentSize)
	if sc.prepare != nil {
		sc.prepare(dry)
	}
	base := dry.fs.Ops()
	if err := sc.work(dry); err != nil {
		t.Fatalf("dry run failed: %v", err)
	}
	n := dry.fs.Ops() - base // before inspect, which lists directories
	if sc.inspect != nil {
		sc.inspect(t, dry)
	}
	if n <= 0 {
		t.Fatal("the workload performed no filesystem operations, so this loop would test nothing")
	}

	ran := 0
	for at := 1; at <= n; at++ {
		for _, after := range []bool{false, true} {
			for _, mode := range modes {
				ran++
				f := newFS(t, mode.seed, faultfs.Options{})
				r := newRig(t, f, sc.policy, sc.segmentSize)
				if sc.prepare != nil {
					sc.prepare(r)
				}
				point := f.Ops() + at
				if after {
					f.CrashAfter(point)
				} else {
					f.CrashAt(point)
				}
				ctx := fmt.Sprintf("op %d of %d (after=%v) mode %s", at, n, after, mode.name)
				err := sc.work(r)
				if !f.Crashed() {
					log := f.Log()
					t.Fatalf("%s: the crash point was never reached (workload returned %v); last operations: %v",
						ctx, err, log[max(0, len(log)-4):])
				}
				f.Crash(mode.opts)
				r.reopen(ctx)
				r.check(ctx)
				if sc.then != nil {
					sc.then(t, r, ctx, mode)
				}
			}
		}
	}
	if want := n * 2 * len(modes); ran != want {
		t.Fatalf("ran %d crash points, want %d", ran, want)
	}
	t.Logf("%d filesystem operations, %d crash points checked", n, ran)
}
