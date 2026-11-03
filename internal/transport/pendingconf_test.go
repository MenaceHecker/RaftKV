package transport

import (
	"fmt"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/MenaceHecker/raftkv/internal/raft"
)

// T3.16: a refused change maps to FailedPrecondition (retry later), never
// Unavailable, and the message tells the caller what to wait for.
func TestPendingConfChangeMessages(t *testing.T) {
	srv := newStubServer(t)

	cases := []struct {
		name string
		err  error
		want []string
	}{
		{
			"an uncommitted change",
			fmt.Errorf("%w: configuration change at index 7 is pending; wait for it to commit, then retry",
				raft.ErrConfChangePending),
			[]string{"index 7", "wait"},
		},
		{
			"a leader that has not committed in its term",
			fmt.Errorf("%w: this leader has not yet committed an entry in its term; retry shortly",
				raft.ErrConfChangePending),
			[]string{"not yet committed an entry in its term"},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			st, ok := status.FromError(srv.translate(c.err))
			if !ok {
				t.Fatalf("translate(%v) did not produce a status", c.err)
			}
			if st.Code() != codes.FailedPrecondition {
				t.Fatalf("code = %v, want FailedPrecondition", st.Code())
			}
			for _, w := range c.want {
				if !strings.Contains(st.Message(), w) {
					t.Errorf("message %q does not contain %q", st.Message(), w)
				}
			}
		})
	}
}
