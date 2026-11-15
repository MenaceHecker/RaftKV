package main

import (
	"reflect"
	"strings"
	"testing"

	"github.com/MenaceHecker/raftkv/internal/gateway"
)

func TestParseNodesAcceptsAWellFormedList(t *testing.T) {
	got, err := parseNodes("1=us-east-1a=10.0.1.10:7000=10.0.1.10:9000,2=us-east-1b=10.0.2.10:7000=10.0.2.10:9000, 3=us-east-1c=node3.internal:7000=node3.internal:9000")
	if err != nil {
		t.Fatalf("parseNodes: %v", err)
	}
	want := []gateway.LiveNode{
		{ID: 1, AZ: "us-east-1a", Addr: "10.0.1.10:7000", AdminAddr: "10.0.1.10:9000"},
		{ID: 2, AZ: "us-east-1b", Addr: "10.0.2.10:7000", AdminAddr: "10.0.2.10:9000"},
		{ID: 3, AZ: "us-east-1c", Addr: "node3.internal:7000", AdminAddr: "node3.internal:9000"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("parseNodes = %+v\nwant %+v", got, want)
	}
}

func TestParseNodesFailsClosed(t *testing.T) {
	ok := func(id string) string { return id + "=az=127.0.0.1:700" + id + "=127.0.0.1:900" + id }
	cases := map[string]string{
		"empty":                "",
		"blanks":               "  ",
		"only a comma":         ",",
		"trailing comma":       ok("1") + ",",
		"leading comma":        "," + ok("1"),
		"empty entry between":  ok("1") + ",," + ok("2"),
		"three fields":         "1=az=127.0.0.1:7001",
		"five fields":          "1=az=127.0.0.1:7001=127.0.0.1:9001=x",
		"no equals":            "1",
		"id zero":              "0=az=127.0.0.1:7001=127.0.0.1:9001",
		"id not a number":      "a=az=127.0.0.1:7001=127.0.0.1:9001",
		"negative id":          "-1=az=127.0.0.1:7001=127.0.0.1:9001",
		"id overflow":          "99999999999999999999=az=127.0.0.1:7001=127.0.0.1:9001",
		"empty id":             "=az=127.0.0.1:7001=127.0.0.1:9001",
		"empty az":             "1==127.0.0.1:7001=127.0.0.1:9001",
		"empty addr":           "1=az==127.0.0.1:9001",
		"empty admin addr":     "1=az=127.0.0.1:7001=",
		"addr without port":    "1=az=127.0.0.1=127.0.0.1:9001",
		"admin without port":   "1=az=127.0.0.1:7001=127.0.0.1",
		"port zero":            "1=az=127.0.0.1:0=127.0.0.1:9001",
		"port too large":       "1=az=127.0.0.1:70000=127.0.0.1:9001",
		"port not a number":    "1=az=127.0.0.1:http=127.0.0.1:9001",
		"addr and admin equal": "1=az=127.0.0.1:7001=127.0.0.1:7001",
		"duplicate id":         ok("1") + "," + "1=az2=127.0.0.1:7002=127.0.0.1:9002",
		"duplicate addr":       ok("1") + "," + "2=az=127.0.0.1:7001=127.0.0.1:9002",
		"duplicate admin addr": ok("1") + "," + "2=az=127.0.0.1:7002=127.0.0.1:9001",
		"admin equals kv addr": ok("1") + "," + "2=az=127.0.0.1:7002=127.0.0.1:7001",
	}
	for name, in := range cases {
		got, err := parseNodes(in)
		if err == nil {
			t.Errorf("%s: %q accepted as %+v", name, in, got)
			continue
		}
		if got != nil {
			t.Errorf("%s: returned nodes %+v along with an error", name, got)
		}
		if strings.Contains(err.Error(), "token") {
			t.Errorf("%s: odd error %v", name, err)
		}
	}
}
