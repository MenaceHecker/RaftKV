package client

import (
	"context"
	"errors"
	"io"
	"time"

	"google.golang.org/grpc"

	raftkvv1 "github.com/MenaceHecker/raftkv/internal/transport/raftkv/v1"
)

// KV is the part of the generated KVService client this package uses.
type KV = raftkvv1.KVServiceClient

// Backoff bounds the delay between attempts.
type Backoff struct {
	Initial    time.Duration
	Max        time.Duration
	Multiplier float64
}

// Config configures New. Zero values get the documented defaults.
type Config struct {
	Addrs        []string
	ClientID     uint64
	StartSeq     uint64
	DialOpts     []grpc.DialOption
	Connect      func(addr string) (KV, io.Closer, error)
	PerAttempt   time.Duration
	DefaultTotal time.Duration
	Backoff      Backoff
	Rand         Rand
	Clock        Clock
}

// NodeStatus is one node's answer to Status.
type NodeStatus struct {
	Addr         string
	ID           uint64
	LeaderID     uint64
	LeaderAddr   string
	Term         uint64
	State        string
	CommitIndex  uint64
	AppliedIndex uint64
}

// Client is a leader-following client for KVService.
type Client struct{ cfg Config }

var errStub = errors.New("client: not implemented")

// New creates a Client. STUB.
func New(cfg Config) (*Client, error) {
	if len(cfg.Addrs) == 0 {
		return nil, errors.New("client: Addrs must not be empty")
	}
	return &Client{cfg: cfg}, nil
}

// ClientID returns the session id used by this client. STUB.
func (c *Client) ClientID() uint64 { return c.cfg.ClientID }

// Get reads a key. STUB.
func (c *Client) Get(_ context.Context, _ string) ([]byte, bool, error) {
	return nil, false, errStub
}

// Put sets a key. STUB.
func (c *Client) Put(_ context.Context, _ string, _ []byte) error { return errStub }

// Delete removes a key. STUB.
func (c *Client) Delete(_ context.Context, _ string) error { return errStub }

// Status asks the nodes in rotation until one answers. STUB.
func (c *Client) Status(context.Context) (NodeStatus, error) { return NodeStatus{}, errStub }

// StatusFrom asks one specific node, once. STUB.
func (c *Client) StatusFrom(_ context.Context, _ string) (NodeStatus, error) {
	return NodeStatus{}, errStub
}

// Close releases every connection. STUB.
func (c *Client) Close() error { return nil }
