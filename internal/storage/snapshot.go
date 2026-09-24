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

const (
	snapshotSuffix = ".snap"
	tempSuffix     = ".tmp"
)

var (
	ErrNoSnapshot = errors.New("storage: no snapshot available")

	ErrSnapshotTooLarge = errors.New("storage: snapshot exceeds the maximum record size")
)

type Snapshot struct {
	Meta SnapshotMeta

	Data []byte

	Conf raft.ConfState
}

type Snapshotter struct {
	dir string
}

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

func snapshotName(meta SnapshotMeta) string {
	return fmt.Sprintf("%016d-%016d%s", uint64(meta.Index), uint64(meta.Term), snapshotSuffix)
}

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

	f, err := os.OpenFile(temp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, fileMode)
	if err != nil {
		return fmt.Errorf("storage: creating snapshot %s: %w", filepath.Base(temp), err)
	}

	if _, err := f.Write(buf); err != nil {
		f.Close()
		os.Remove(temp)
		return fmt.Errorf("storage: writing snapshot: %w", err)
	}

	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(temp)
		return fmt.Errorf("storage: syncing snapshot: %w", err)
	}
	if err := f.Close(); err != nil {
		os.Remove(temp)
		return fmt.Errorf("storage: closing snapshot: %w", err)
	}

	if err := os.Rename(temp, final); err != nil {
		os.Remove(temp)
		return fmt.Errorf("storage: publishing snapshot: %w", err)
	}

	return syncDir(s.dir)
}

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
			continue
		}
		metas = append(metas, meta)
	}

	sort.Slice(metas, func(i, j int) bool {
		if metas[i].Index != metas[j].Index {
			return metas[i].Index > metas[j].Index
		}
		return metas[i].Term > metas[j].Term
	})
	return metas, nil
}

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
		return Snapshot{}, fmt.Errorf("storage: snapshot %s contains metadata %+v", name, got)
	}

	return Snapshot{Meta: got, Data: body, Conf: conf}, nil
}

func appendConfState(dst []byte, cs raft.ConfState) []byte {
	dst = appendUint64(dst, uint64(len(cs.Voters)))
	for _, id := range cs.Voters {
		dst = appendUint64(dst, uint64(id))
	}

	dst = appendUint64(dst, uint64(len(cs.Incoming)))
	for _, id := range cs.Incoming {
		dst = appendUint64(dst, uint64(id))
	}

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
