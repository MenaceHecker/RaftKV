package main

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/MenaceHecker/raftkv/internal/gateway"
)

// parseNodes parses --nodes: "id=az=addr=adminaddr,id=az=addr=adminaddr,...".
// addr is the node's gRPC address (KVService), adminaddr its AdminService
// address. It fails closed: an empty list, an empty entry, a missing or extra
// field, a zero or repeated id, a repeated address, or an address that is not
// host:port with a port in 1..65535 is an error, and no node is returned.
// Errors name the entry number, never an address.
func parseNodes(s string) ([]gateway.LiveNode, error) {
	if strings.TrimSpace(s) == "" {
		return nil, errors.New("--nodes is empty")
	}
	var out []gateway.LiveNode
	ids := map[uint64]bool{}
	addrs := map[string]bool{}
	for i, entry := range strings.Split(s, ",") {
		entry = strings.TrimSpace(entry)
		n := i + 1
		if entry == "" {
			return nil, fmt.Errorf("--nodes: entry %d is empty", n)
		}
		f := strings.Split(entry, "=")
		if len(f) != 4 {
			return nil, fmt.Errorf("--nodes: entry %d must be id=az=addr=adminaddr", n)
		}
		id, err := strconv.ParseUint(strings.TrimSpace(f[0]), 10, 64)
		if err != nil || id == 0 {
			return nil, fmt.Errorf("--nodes: entry %d: the id must be a positive integer", n)
		}
		if ids[id] {
			return nil, fmt.Errorf("--nodes: entry %d repeats node id %d", n, id)
		}
		ids[id] = true
		node := gateway.LiveNode{
			ID: id, AZ: strings.TrimSpace(f[1]), Addr: strings.TrimSpace(f[2]), AdminAddr: strings.TrimSpace(f[3]),
		}
		if node.AZ == "" {
			return nil, fmt.Errorf("--nodes: entry %d has an empty availability zone", n)
		}
		for what, a := range map[string]string{"addr": node.Addr, "adminaddr": node.AdminAddr} {
			if err := checkHostPort(a); err != nil {
				return nil, fmt.Errorf("--nodes: entry %d: %s: %w", n, what, err)
			}
			if addrs[a] {
				return nil, fmt.Errorf("--nodes: entry %d reuses an address already listed", n)
			}
			addrs[a] = true
		}
		if node.Addr == node.AdminAddr {
			return nil, fmt.Errorf("--nodes: entry %d uses one address for both gRPC and admin", n)
		}
		out = append(out, node)
	}
	return out, nil
}

func checkHostPort(a string) error {
	host, port, err := net.SplitHostPort(a)
	if err != nil || host == "" {
		return errors.New("must be host:port")
	}
	p, err := strconv.ParseUint(port, 10, 16)
	if err != nil || p == 0 {
		return errors.New("the port must be a number in 1..65535")
	}
	return nil
}
