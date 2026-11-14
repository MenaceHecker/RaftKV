package transport

import (
	"bytes"
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	raftkvv1 "github.com/MenaceHecker/raftkv/internal/transport/raftkv/v1"
)

// budgetServer serves a RaftServer whose budget and per-stream limit are set
// before it starts.
func budgetServer(t *testing.T, maxBytes int64, maxStreams, maxSnapshot int) (raftkvv1.RaftServiceClient, *recordingStepper, *snapshotBudget) {
	t.Helper()
	stepper := &recordingStepper{}
	srv, err := NewRaftServer(stepper)
	if err != nil {
		t.Fatal(err)
	}
	srv.budget = newSnapshotBudget(maxBytes, maxStreams)
	if maxSnapshot > 0 {
		srv.maxSnapshot = maxSnapshot
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer()
	srv.Register(gs)
	go func() { _ = gs.Serve(l) }()
	t.Cleanup(gs.Stop)
	cc, err := grpc.NewClient(l.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cc.Close() })
	return raftkvv1.NewRaftServiceClient(cc), stepper, srv.budget
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(grpcSettleTimeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func awaitIdle(t *testing.T, b *snapshotBudget) {
	t.Helper()
	waitFor(t, "the budget to return to zero", func() bool {
		u := b.usage()
		return u.Bytes == 0 && u.Streams == 0
	})
}

func finish(stream raftkvv1.RaftService_DeliverSnapshotClient) error {
	_, err := stream.CloseAndRecv()
	return err
}

func TestSnapshotStreamsRespectTheAggregateByteBudget(t *testing.T) {
	const (
		frame   = 16 << 10
		budget  = 4 * frame
		streams = 4
		frames  = 3
	)
	client, _, b := budgetServer(t, budget, 100, 0)

	gate := make(chan struct{})
	errs := make(chan error, streams)
	var sent sync.WaitGroup
	for range streams {
		sent.Add(1)
		go func() {
			stream, err := client.DeliverSnapshot(context.Background())
			if err != nil {
				sent.Done()
				errs <- err
				return
			}
			_ = stream.Send(header(1, 1))
			for range frames {
				_ = stream.Send(dataFrame(bytes.Repeat([]byte("s"), frame)))
			}
			sent.Done()
			<-gate
			errs <- finish(stream)
		}()
	}
	sent.Wait()
	// All streams stay open, so nothing is released while demand (12 frames)
	// is three times the budget (4 frames).
	waitFor(t, "an over-budget stream to be refused", func() bool { return b.usage().Rejected > 0 })
	close(gate)

	exhausted := 0
	for range streams {
		if err := <-errs; status.Code(err) == codes.ResourceExhausted {
			exhausted++
		}
	}
	if exhausted == 0 {
		t.Error("no stream was refused with ResourceExhausted")
	}
	if u := b.usage(); u.PeakBytes > budget {
		t.Errorf("peak in-flight bytes %d exceeded the budget %d", u.PeakBytes, budget)
	}
	awaitIdle(t, b)
}

func TestSnapshotStreamCountIsBounded(t *testing.T) {
	const maxStreams = 2
	client, _, b := budgetServer(t, 1<<20, maxStreams, 0)

	open := make([]raftkvv1.RaftService_DeliverSnapshotClient, 0, maxStreams)
	for range maxStreams {
		s, err := client.DeliverSnapshot(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Send(header(1, 1)); err != nil {
			t.Fatal(err)
		}
		open = append(open, s)
	}
	waitFor(t, "both streams to hold a slot", func() bool { return b.usage().Streams == maxStreams })

	extra, err := client.DeliverSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	_ = extra.Send(header(1, 1))
	if got := status.Code(finish(extra)); got != codes.ResourceExhausted {
		t.Errorf("stream over the limit: code %s, want ResourceExhausted", got)
	}
	if u := b.usage(); u.PeakStreams > maxStreams {
		t.Errorf("peak concurrent streams %d exceeded %d", u.PeakStreams, maxStreams)
	}
	for _, s := range open {
		if err := finish(s); err != nil {
			t.Errorf("admitted stream failed: %v", err)
		}
	}
	awaitIdle(t, b)
}

func TestBudgetIsReleasedOnErrorCancelAndEarlyClose(t *testing.T) {
	client, _, b := budgetServer(t, 1<<20, 4, 0)
	chunk := bytes.Repeat([]byte("r"), 4<<10)

	exits := map[string]func(){
		"protocol error": func() {
			s, _ := client.DeliverSnapshot(context.Background())
			_ = s.Send(header(1, 1))
			_ = s.Send(dataFrame(chunk))
			_ = s.Send(header(2, 2)) // a second header is a protocol error
			_ = finish(s)
		},
		"cancel": func() {
			ctx, cancel := context.WithCancel(context.Background())
			s, _ := client.DeliverSnapshot(ctx)
			_ = s.Send(header(1, 1))
			_ = s.Send(dataFrame(chunk))
			cancel()
		},
		"early close without a header": func() {
			s, _ := client.DeliverSnapshot(context.Background())
			_ = s.Send(dataFrame(chunk))
			_ = finish(s)
		},
		"early close after data": func() {
			s, _ := client.DeliverSnapshot(context.Background())
			_ = s.Send(dataFrame(chunk))
			_ = s.CloseSend()
			_ = finish(s)
		},
		"success": func() {
			s, _ := client.DeliverSnapshot(context.Background())
			_ = s.Send(header(1, 1))
			_ = s.Send(dataFrame(chunk))
			if err := finish(s); err != nil {
				t.Errorf("success path failed: %v", err)
			}
		},
	}
	for name, exit := range exits {
		for range 10 {
			exit()
		}
		awaitIdle(t, b)
		if t.Failed() {
			t.Fatalf("after %q", name)
		}
	}
	if u := b.usage(); u.PeakBytes == 0 {
		t.Error("the budget never recorded a reservation; the streams were not accounted")
	}
}

func TestOversizeSingleStreamIsStillRejected(t *testing.T) {
	const limit = 1 << 20
	client, stepper, b := budgetServer(t, 64<<20, 4, limit)

	s, err := client.DeliverSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	_ = s.Send(header(1, 1))
	chunk := bytes.Repeat([]byte("o"), 256<<10)
	for range 8 { // 2 MiB against a 1 MiB limit
		if s.Send(dataFrame(chunk)) != nil {
			break
		}
	}
	if err := finish(s); err == nil {
		t.Fatal("a stream over the per-stream limit was accepted")
	}
	if stepper.count() != 0 {
		t.Error("the node was stepped for an oversize stream")
	}
	if u := b.usage(); u.PeakBytes > limit {
		t.Errorf("buffered %d bytes past the %d byte limit", u.PeakBytes, limit)
	}
	awaitIdle(t, b)
}

func TestRejectedSnapshotStreamDoesNotBufferPayload(t *testing.T) {
	client, _, b := budgetServer(t, 1<<20, 1, 0)

	holder, err := client.DeliverSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	_ = holder.Send(header(1, 1))
	waitFor(t, "the only slot to be taken", func() bool { return b.usage().Streams == 1 })
	before := b.usage().PeakBytes

	refused, err := client.DeliverSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	_ = refused.Send(header(2, 2))
	big := bytes.Repeat([]byte("p"), 512<<10)
	for range 4 {
		if refused.Send(dataFrame(big)) != nil {
			break
		}
	}
	if got := status.Code(finish(refused)); got != codes.ResourceExhausted {
		t.Fatalf("refused stream: code %s, want ResourceExhausted", got)
	}
	if after := b.usage().PeakBytes; after != before {
		t.Errorf("a refused stream reserved or buffered %d bytes", after-before)
	}
	if err := finish(holder); err != nil {
		t.Errorf("admitted stream failed: %v", err)
	}
	awaitIdle(t, b)
}

func TestInstallSnapshotStillWorksUnderBudget(t *testing.T) {
	client, stepper, b := budgetServer(t, DefaultSnapshotBudgetBytes, DefaultSnapshotStreams, 0)
	payload := bytes.Repeat([]byte("d"), 3*SnapshotChunkSize)

	s, err := client.DeliverSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	_ = s.Send(header(5, 2))
	for off := 0; off < len(payload); off += SnapshotChunkSize {
		if err := s.Send(dataFrame(payload[off : off+SnapshotChunkSize])); err != nil {
			t.Fatal(err)
		}
	}
	if err := finish(s); err != nil {
		t.Fatalf("a snapshot under the default budget failed: %v", err)
	}
	if stepper.count() != 1 || !bytes.Equal(stepper.msgs[0].Snapshot.Data, payload) {
		t.Error("the snapshot was not delivered intact")
	}
	awaitIdle(t, b)
}
