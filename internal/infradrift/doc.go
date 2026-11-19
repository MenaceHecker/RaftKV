// Package infradrift holds a test only: it fails when a systemd unit template under
// infra/modules/compute uses a command line flag that the real raftkv-server or gateway does
// not define, or leaves out a token file flag that binary requires. It has no code.
package infradrift
