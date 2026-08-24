package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/MenaceHecker/raftkv/internal/raft"
)

// Snapshots.
//
// A snapshot is the state machine's entire contents at one point in the log,
// written as a single self-contained file. It exists so the log does not grow
// without bound: once a snapshot covers everything through index N, the log
// entries at or below N can be deleted, because replaying them would only
// reproduce state the snapshot already holds.
//
// The correctness requirement is that a snapshot is never observed
// half-written. That is achieved with the usual atomic-rename dance — write a
// temporary file, fsync it, rename it into place, fsync the directory — so a
// crash at any point leaves either no snapshot or a complete one, never a
// partial file under a name that recovery would trust.

const (
	snapshotSuffix = ".snap"
	// tempSuffix marks a snapshot still being written. Files carrying it are
	// swept away at startup: their presence means a crash interrupted a save,
	// and a partial snapshot is worth nothing.
	tempSuffix = ".tmp"
)

var (
	// ErrNoSnapshot means no usable snapshot exists yet, which is the normal
	// state of a node that has not taken one.
	ErrNoSnapshot = errors.New("storage: no snapshot available")

	// ErrSnapshotTooLarge means the encoded snapshot exceeds what a single
	// record can hold. See the note on Save.
	ErrSnapshotTooLarge = errors.New("storage: snapshot exceeds the maximum record size")
)

// Snapshot is a point-in-time image of the state machine.
type Snapshot struct {
	// Meta says which log position the image corresponds to. A node that
	// loads this snapshot has, by definition, applied every entry through
	// Meta.Index.
	Meta SnapshotMeta

	// Data is the state machine's serialized contents. The storage layer
	// treats it as opaque; only the state machine knows how to read it.
	Data []byte

	// Conf is the cluster membership as of Meta.Index.
	//
	// It travels with the snapshot because it cannot be recovered any other
	// way. Membership lives in the log as conf-change entries, and a snapshot
	// exists precisely so those entries can be deleted — so without recording
	// the configuration here, compacting past a membership change would lose
	// it, and the node would come back believing in a cluster that no longer
	// exists.
	Conf raft.ConfState
}

// Snapshotter manages the snapshot files in a directory.
//
// It keeps more than one on purpose. A snapshot that fails to load is not
// necessarily a disaster if an older one is still intact — the node can
// recover from the older image and replay the log forward from there, which
// is strictly better than refusing to start.
type Snapshotter struct {
	dir string
}

// NewSnapshotter prepares a directory for snapshots, creating it if needed and
// sweeping away any temporary files left by an interrupted save.
func NewSnapshotter(dir string) (*Snapshotter, error) {
	if dir == "" {
		return nil, errors.New("storage: snapshot directory must not be empty")
	}
	if err := os.MkdirAll(dir, dirMode); err != nil {
		return nil, fmt.Errorf("storage: creating snapshot directory: %w", err)
	}

	s := &Snapshotter{dir: dir}
	if err := s.sweepTemporaries(); err != nil {
		return nil, err
	}
	return s, nil
}

// sweepTemporaries removes partially written snapshots. A file still carrying
// the temporary suffix was never renamed into place, so the save that produced
// it did not complete.
func (s *Snapshotter) sweepTemporaries() error {
	matches, err := filepath.Glob(filepath.Join(s.dir, "*"+snapshotSuffix+tempSuffix))
	if err != nil {
		return fmt.Errorf("storage: scanning for partial snapshots: %w", err)
	}
	if len(matches) == 0 {
		return nil
	}

	for _, path := range matches {
		if err := os.Remove(path); err != nil {
			return fmt.Errorf("storage: removing partial snapshot %s: %w", filepath.Base(path), err)
		}
	}
	return syncDir(s.dir)
}

// snapshotName builds the filename for a snapshot. Zero padding makes lexical
// order match numeric order, so a directory listing reads chronologically.
func snapshotName(meta SnapshotMeta) string {
	return fmt.Sprintf("%016d-%016d%s", uint64(meta.Index), uint64(meta.Term), snapshotSuffix)
}

// parseSnapshotName recovers the metadata encoded in a snapshot's filename.
func parseSnapshotName(name string) (SnapshotMeta, error) {
	base := strings.TrimSuffix(name, snapshotSuffix)
	idxStr, termStr, ok := strings.Cut(base, "-")
	if !ok {
		return SnapshotMeta{}, errors.New("expected a name of the form <index>-<term>.snap")
	}

	var idx, term uint64
	if _, err := fmt.Sscanf(idxStr, "%d", &idx); err != nil {
		return SnapshotMeta{}, fmt.Errorf("parsing snapshot index: %w", err)
	}
	if _, err := fmt.Sscanf(termStr, "%d", &term); err != nil {
		return SnapshotMeta{}, fmt.Errorf("parsing snapshot term: %w", err)
	}

	return SnapshotMeta{Index: raft.Index(idx), Term: raft.Term(term)}, nil
}

// Save writes a snapshot atomically.
//
// The whole image is held in memory and written as one record. That bounds a
// snapshot at maxRecordSize, which is ample for the key-value state machine
// this stores but would not be for a large one. Streaming a snapshot in chunks
// is the fix, and is deliberately left for later — the limit is enforced here
// with a clear error rather than discovered as a corrupt file at recovery time.
func (s *Snapshotter) Save(snap Snapshot) error {
	payload := appendUint64(nil, uint64(snap.Meta.Index))
	payload = appendUint64(payload, uint64(snap.Meta.Term))
	payload = appendBytes(payload, snap.Data)
	payload = appendConfState(payload, snap.Conf)

	if len(payload)+typeSize > maxRecordSize {
		return fmt.Errorf("%w: %d bytes at index %d exceeds the %d-byte limit",
			ErrSnapshotTooLarge, len(snap.Data), snap.Meta.Index, maxRecordSize)
	}

	buf := appendRecord(nil, recordSnapshotMeta, payload)

	final := filepath.Join(s.dir, snapshotName(snap.Meta))
	temp := final + tempSuffix

	// Write under a temporary name first. Until the rename, a crash leaves
	// only a .tmp file, which the next startup sweeps away.
	f, err := os.OpenFile(temp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, fileMode)
	if err != nil {
		return fmt.Errorf("storage: creating snapshot %s: %w", filepath.Base(temp), err)
	}

	if _, err := f.Write(buf); err != nil {
		f.Close()
		os.Remove(temp)
		return fmt.Errorf("storage: writing snapshot: %w", err)
	}

	// The contents must be durable before the rename. Renaming first would
	// publish a name that a crash could leave pointing at an empty file.
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(temp)
		return fmt.Errorf("storage: syncing snapshot: %w", err)
	}
	if err := f.Close(); err != nil {
		os.Remove(temp)
		return fmt.Errorf("storage: closing snapshot: %w", err)
	}

	// Rename is atomic within a directory, so the snapshot appears under its
	// real name complete or not at all.
	if err := os.Rename(temp, final); err != nil {
		os.Remove(temp)
		return fmt.Errorf("storage: publishing snapshot: %w", err)
	}

	// And the rename itself is a directory change, durable only once the
	// directory is synced.
	return syncDir(s.dir)
}

// List returns the metadata of every snapshot present, newest first.
func (s *Snapshotter) List() ([]SnapshotMeta, error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, fmt.Errorf("storage: reading snapshot directory: %w", err)
	}

	var metas []SnapshotMeta
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, snapshotSuffix) {
			continue
		}
		meta, err := parseSnapshotName(name)
		if err != nil {
			// Unlike the WAL directory, an unparseable name here is not
			// fatal: snapshots are redundant with the log, so an unknown file
			// is skipped rather than blocking startup.
			continue
		}
		metas = append(metas, meta)
	}

	// Newest first, so a loader tries the most recent image before falling
	// back to older ones.
	sort.Slice(metas, func(i, j int) bool {
		if metas[i].Index != metas[j].Index {
			return metas[i].Index > metas[j].Index
		}
		return metas[i].Term > metas[j].Term
	})
	return metas, nil
}

// Load returns the most recent snapshot that can actually be read.
//
// If the newest file is damaged it falls back to the next one rather than
// failing. An older snapshot plus the log entries that follow it reconstructs
// exactly the same state, so recovering from a stale image costs replay time
// and nothing else — refusing to start would be the worse answer.
func (s *Snapshotter) Load() (Snapshot, error) {
	metas, err := s.List()
	if err != nil {
		return Snapshot{}, err
	}
	if len(metas) == 0 {
		return Snapshot{}, ErrNoSnapshot
	}

	var firstErr error
	for _, meta := range metas {
		snap, err := s.load(meta)
		if err == nil {
			return snap, nil
		}
		if firstErr == nil {
			firstErr = err
		}
	}

	return Snapshot{}, fmt.Errorf("storage: no readable snapshot among %d candidates: %w",
		len(metas), firstErr)
}

// LoadAt reads one specific snapshot.
func (s *Snapshotter) LoadAt(meta SnapshotMeta) (Snapshot, error) {
	return s.load(meta)
}

func (s *Snapshotter) load(meta SnapshotMeta) (Snapshot, error) {
	name := snapshotName(meta)

	data, err := os.ReadFile(filepath.Join(s.dir, name))
	if err != nil {
		return Snapshot{}, fmt.Errorf("storage: reading snapshot %s: %w", name, err)
	}

	typ, payload, _, err := readRecord(data)
	if err != nil {
		return Snapshot{}, fmt.Errorf("storage: snapshot %s: %w", name, err)
	}
	if typ != recordSnapshotMeta {
		return Snapshot{}, fmt.Errorf("storage: snapshot %s holds a %s record", name, typ)
	}

	r := &reader{b: payload}
	index, err := r.uint64()
	if err != nil {
		return Snapshot{}, fmt.Errorf("storage: snapshot %s index: %w", name, err)
	}
	term, err := r.uint64()
	if err != nil {
		return Snapshot{}, fmt.Errorf("storage: snapshot %s term: %w", name, err)
	}
	body, err := r.bytes()
	if err != nil {
		return Snapshot{}, fmt.Errorf("storage: snapshot %s data: %w", name, err)
	}
	conf, err := readConfState(r)
	if err != nil {
		return Snapshot{}, fmt.Errorf("storage: snapshot %s configuration: %w", name, err)
	}

	got := SnapshotMeta{Index: raft.Index(index), Term: raft.Term(term)}
	if got != meta {
		// The filename and the contents disagree, so one of them was
		// tampered with or the file was renamed by hand. Neither is safe to
		// guess about.
		return Snapshot{}, fmt.Errorf("storage: snapshot %s contains metadata %+v", name, got)
	}

	return Snapshot{Meta: got, Data: body, Conf: conf}, nil
}

// appendConfState writes a cluster configuration.
//
// Both voter sets arrive already sorted, and the address map is sorted here,
// so identical membership always produces identical bytes. Two replicas'
// snapshots stay directly comparable, which is the cheapest check that they
// really did converge.
func appendConfState(dst []byte, cs raft.ConfState) []byte {
	dst = appendUint64(dst, uint64(len(cs.Voters)))
	for _, id := range cs.Voters {
		dst = appendUint64(dst, uint64(id))
	}

	dst = appendUint64(dst, uint64(len(cs.Incoming)))
	for _, id := range cs.Incoming {
		dst = appendUint64(dst, uint64(id))
	}

	// The joint flag is recorded rather than inferred from the incoming set,
	// because a shrinking transition can leave that set smaller than the
	// outgoing one and, at the limit, empty.
	var joint uint64
	if cs.Joint {
		joint = 1
	}
	dst = appendUint64(dst, joint)

	ids := make([]raft.NodeID, 0, len(cs.Addrs))
	for id := range cs.Addrs {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	dst = appendUint64(dst, uint64(len(ids)))
	for _, id := range ids {
		dst = appendUint64(dst, uint64(id))
		dst = appendBytes(dst, []byte(cs.Addrs[id]))
	}
	return dst
}

// readConfState reads a cluster configuration written by appendConfState.
func readConfState(r *reader) (raft.ConfState, error) {
	readIDs := func(what string) ([]raft.NodeID, error) {
		count, err := r.uint64()
		if err != nil {
			return nil, fmt.Errorf("reading %s count: %w", what, err)
		}
		if count > maxRecordSize {
			return nil, fmt.Errorf("implausible %s count %d", what, count)
		}
		if count == 0 {
			return nil, nil
		}
		out := make([]raft.NodeID, 0, count)
		for i := uint64(0); i < count; i++ {
			id, err := r.uint64()
			if err != nil {
				return nil, fmt.Errorf("reading %s member %d: %w", what, i, err)
			}
			out = append(out, raft.NodeID(id))
		}
		return out, nil
	}

	var cs raft.ConfState
	var err error

	if cs.Voters, err = readIDs("voters"); err != nil {
		return raft.ConfState{}, err
	}
	if cs.Incoming, err = readIDs("incoming voters"); err != nil {
		return raft.ConfState{}, err
	}

	joint, err := r.uint64()
	if err != nil {
		return raft.ConfState{}, fmt.Errorf("reading joint flag: %w", err)
	}
	cs.Joint = joint != 0

	count, err := r.uint64()
	if err != nil {
		return raft.ConfState{}, fmt.Errorf("reading address count: %w", err)
	}
	if count > maxRecordSize {
		return raft.ConfState{}, fmt.Errorf("implausible address count %d", count)
	}
	if count > 0 {
		cs.Addrs = make(map[raft.NodeID]string, count)
		for i := uint64(0); i < count; i++ {
			id, err := r.uint64()
			if err != nil {
				return raft.ConfState{}, fmt.Errorf("reading address %d node ID: %w", i, err)
			}
			addr, err := r.bytes()
			if err != nil {
				return raft.ConfState{}, fmt.Errorf("reading address %d: %w", i, err)
			}
			cs.Addrs[raft.NodeID(id)] = string(addr)
		}
	}

	return cs, nil
}

// Purge deletes all but the newest keep snapshots.
//
// Keeping more than one is the whole reason Load can fall back, so keep must
// be at least one and a request for zero is treated as a mistake rather than
// an instruction to delete everything.
func (s *Snapshotter) Purge(keep int) error {
	if keep < 1 {
		return fmt.Errorf("storage: Purge must keep at least one snapshot, got %d", keep)
	}

	metas, err := s.List()
	if err != nil {
		return err
	}
	if len(metas) <= keep {
		return nil
	}

	for _, meta := range metas[keep:] {
		path := filepath.Join(s.dir, snapshotName(meta))
		if err := os.Remove(path); err != nil {
			return fmt.Errorf("storage: removing old snapshot %s: %w", snapshotName(meta), err)
		}
	}
	return syncDir(s.dir)
}
