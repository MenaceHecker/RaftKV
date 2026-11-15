package client

import (
	"errors"
	"fmt"

	"google.golang.org/grpc/codes"
)

var (
	// ErrClosed is returned (wrapped in a DefinitiveError for writes) for any
	// use of a Client after Close.
	ErrClosed = errors.New("client: closed")

	// ErrUnavailable means no node answered usefully before the deadline. For a
	// read it is the whole story: nothing was changed. Test with errors.Is.
	ErrUnavailable = errors.New("client: no leader reached")
)

// DefinitiveError reports a write that cannot have been applied: no attempt
// reached a server, or a server refused it outright (InvalidArgument,
// PermissionDenied, Unauthenticated, Unimplemented, ResourceExhausted). It
// also wraps the failure of a read. Safe to report as "not done".
type DefinitiveError struct {
	// Code is the gRPC code when a server refused the request, else
	// codes.Unknown or codes.Unavailable for client-side failures.
	Code codes.Code
	Err  error
}

func (e *DefinitiveError) Error() string {
	return fmt.Sprintf("client: definitive failure (%s): %v", e.Code, e.Err)
}

func (e *DefinitiveError) Unwrap() error { return e.Err }

// IndeterminateError reports a write whose outcome is unknown: at least one
// attempt was sent and the operation ended without a definite answer. The
// write may or may not have been applied.
type IndeterminateError struct {
	// Attempts is the number of attempts that were sent.
	Attempts int
	// Seq is the sequence number used by every attempt of this write.
	Seq uint64
	// Cause is the last failure, or the context error that ended the operation.
	Cause error
}

func (e *IndeterminateError) Error() string {
	return fmt.Sprintf("client: outcome unknown after %d attempt(s) (seq %d): %v", e.Attempts, e.Seq, e.Cause)
}

func (e *IndeterminateError) Unwrap() error { return e.Cause }

// IsIndeterminate reports whether err is a write whose outcome is unknown.
func IsIndeterminate(err error) bool {
	var ie *IndeterminateError
	return errors.As(err, &ie)
}
