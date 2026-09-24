package transport

import (
	"bytes"
	"context"
	"net"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/MenaceHecker/raftkv/internal/raft"
	raftkvv1 "github.com/MenaceHecker/raftkv/internal/transport/raftkv/v1"
)

func snapshotServer(t *testing.T) (raftkvv1.RaftServiceClient, *recordingStepper) {
	t.Helper()

	stepper := &recordingStepper{}
	srv, err := NewRaftServer(stepper)
	if err != nil {
		t.Fatalf("creating server: %v", err)
	}

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listening: %v", err)
	}
	gs := grpc.NewServer()
	srv.Register(gs)
	go gs.Serve(l)
	t.Cleanup(gs.Stop)

	cc, err := grpc.NewClient(l.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dialing: %v", err)
	}
	t.Cleanup(func() { cc.Close() })

	return raftkvv1.NewRaftServiceClient(cc), stepper
}

func header(index, term uint64) *raftkvv1.SnapshotChunk {
	return &raftkvv1.SnapshotChunk{
		Frame: &raftkvv1.SnapshotChunk_Header{
			Header: &raftkvv1.Message{
				Type: raftkvv1.MessageType_MESSAGE_TYPE_INSTALL_SNAPSHOT,
				From: 1, To: 2, Term: term,
				Snapshot: &raftkvv1.Snapshot{
					Index: index,
					Term:  term,
					Conf:  &raftkvv1.ConfState{Voters: []uint64{1, 2, 3}},
				},
			},
		},
	}
}

func dataFrame(b []byte) *raftkvv1.SnapshotChunk {
	return &raftkvv1.SnapshotChunk{Frame: &raftkvv1.SnapshotChunk_Data{Data: b}}
}

func TestStreamedSnapshotIsReassembledInOrder(t *testing.T) {
	client, stepper := snapshotServer(t)

	pieces := [][]byte{
		bytes.Repeat([]byte("a"), 1000),
		bytes.Repeat([]byte("b"), 1000),
		bytes.Repeat([]byte("c"), 7),
	}

	stream, err := client.DeliverSnapshot(context.Background())
	if err != nil {
		t.Fatalf("opening stream: %v", err)
	}
	if err := stream.Send(header(42, 7)); err != nil {
		t.Fatalf("sending header: %v", err)
	}
	for _, p := range pieces {
		if err := stream.Send(dataFrame(p)); err != nil {
			t.Fatalf("sending data: %v", err)
		}
	}
	if _, err := stream.CloseAndRecv(); err != nil {
		t.Fatalf("closing stream: %v", err)
	}

	if got := stepper.count(); got != 1 {
		t.Fatalf("the node was stepped %d times, want exactly 1", got)
	}
	m := stepper.msgs[0]
	if m.Snapshot == nil {
		t.Fatal("the stepped message carries no snapshot")
	}
	want := bytes.Join(pieces, nil)
	if !bytes.Equal(m.Snapshot.Data, want) {
		t.Errorf("reassembled %d bytes, want %d", len(m.Snapshot.Data), len(want))
	}
	if m.Snapshot.Index != 42 || m.Snapshot.Term != 7 {
		t.Errorf("snapshot is at %d/%d, want 42/7", m.Snapshot.Index, m.Snapshot.Term)
	}
	if len(m.Snapshot.Conf.Voters) != 3 {
		t.Errorf("configuration did not survive the header: %v", m.Snapshot.Conf.Voters)
	}
}

func TestSnapshotStreamIsRefusedWithoutAHeader(t *testing.T) {
	client, stepper := snapshotServer(t)

	stream, err := client.DeliverSnapshot(context.Background())
	if err != nil {
		t.Fatalf("opening stream: %v", err)
	}
	if err := stream.Send(dataFrame([]byte("orphan"))); err != nil {
		t.Fatalf("sending data: %v", err)
	}
	_, err = stream.CloseAndRecv()
	if err == nil {
		t.Fatal("the server accepted snapshot data with no header")
	}
	if !strings.Contains(err.Error(), "before its header") {
		t.Errorf("error was %q, want it to name the missing header", err)
	}
	if got := stepper.count(); got != 0 {
		t.Errorf("the node was stepped %d times for a rejected stream", got)
	}
}

func TestEmptySnapshotStreamIsRefused(t *testing.T) {
	client, stepper := snapshotServer(t)

	stream, err := client.DeliverSnapshot(context.Background())
	if err != nil {
		t.Fatalf("opening stream: %v", err)
	}
	if _, err := stream.CloseAndRecv(); err == nil {
		t.Fatal("the server accepted a stream carrying nothing")
	}
	if got := stepper.count(); got != 0 {
		t.Errorf("the node was stepped %d times for an empty stream", got)
	}
}

func TestSecondHeaderIsRefused(t *testing.T) {
	client, stepper := snapshotServer(t)

	stream, err := client.DeliverSnapshot(context.Background())
	if err != nil {
		t.Fatalf("opening stream: %v", err)
	}
	if err := stream.Send(header(42, 7)); err != nil {
		t.Fatalf("sending header: %v", err)
	}
	if err := stream.Send(dataFrame([]byte("x"))); err != nil {
		t.Fatalf("sending data: %v", err)
	}
	_ = stream.Send(header(99, 9))
	if _, err := stream.CloseAndRecv(); err == nil {
		t.Fatal("the server accepted two headers on one stream")
	}
	if got := stepper.count(); got != 0 {
		t.Errorf("the node was stepped %d times for a rejected stream", got)
	}
}

func TestSnapshotStreamStopsAtTheSizeLimit(t *testing.T) {
	if MaxSnapshotBytes < SnapshotChunkSize {
		t.Fatalf("MaxSnapshotBytes %d is below one chunk", MaxSnapshotBytes)
	}

	client, stepper := snapshotServer(t)
	stream, err := client.DeliverSnapshot(context.Background())
	if err != nil {
		t.Fatalf("opening stream: %v", err)
	}
	if err := stream.Send(header(1, 1)); err != nil {
		t.Fatalf("sending header: %v", err)
	}

	chunk := bytes.Repeat([]byte("z"), SnapshotChunkSize)
	var sendErr error
	for sent := 0; sent <= MaxSnapshotBytes; sent += len(chunk) {
		if sendErr = stream.Send(dataFrame(chunk)); sendErr != nil {
			break
		}
	}
	if _, err := stream.CloseAndRecv(); err == nil {
		t.Fatal("the server accepted a snapshot past its size limit")
	}
	if got := stepper.count(); got != 0 {
		t.Errorf("the node was stepped %d times for an oversized stream", got)
	}
}

func TestNonSnapshotMessagesStillUseTheUnaryCall(t *testing.T) {
	client, stepper := snapshotServer(t)

	m, err := MessageToWire(raft.Message{
		Type: raft.MsgHeartbeat, From: 1, To: 2, Term: 3,
	})
	if err != nil {
		t.Fatalf("encoding: %v", err)
	}
	if _, err := client.Deliver(context.Background(),
		&raftkvv1.DeliverRequest{Message: m}); err != nil {
		t.Fatalf("delivering: %v", err)
	}
	if got := stepper.count(); got != 1 {
		t.Fatalf("the node was stepped %d times, want 1", got)
	}
}
