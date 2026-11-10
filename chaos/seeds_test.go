package chaos

import (
	"reflect"
	"strings"
	"testing"
)

func TestParseSeedsAccepts(t *testing.T) {
	got, err := ParseSeeds("1-200")
	if err != nil {
		t.Fatalf("ParseSeeds(%q) failed: %v", "1-200", err)
	}
	if len(got) != 200 {
		t.Fatalf("ParseSeeds(%q) gave %d seeds, want 200", "1-200", len(got))
	}
	for i, s := range got {
		if s != int64(i+1) {
			t.Fatalf("seed %d is %d, want ascending values from 1", i, s)
		}
	}

	cases := []struct {
		spec string
		want []int64
	}{
		{"7", []int64{7}},
		{"1,3,5-6", []int64{1, 3, 5, 6}},
	}
	for _, tc := range cases {
		got, err := ParseSeeds(tc.spec)
		if err != nil {
			t.Errorf("ParseSeeds(%q) failed: %v", tc.spec, err)
			continue
		}
		if !reflect.DeepEqual(got, tc.want) {
			t.Errorf("ParseSeeds(%q) = %v, want %v", tc.spec, got, tc.want)
		}
	}
}

func TestParseSeedsRejects(t *testing.T) {
	cases := []struct {
		spec string
		want string
	}{
		{"", "empty"},
		{"0", "must be at least 1"},
		{"5-1", "descending"},
		{"a", "invalid"},
		{"1-", "malformed"},
		{"1-999999", "too many"},
		{"1,,2", "empty"},
	}
	for _, tc := range cases {
		got, err := ParseSeeds(tc.spec)
		if err == nil {
			t.Errorf("ParseSeeds(%q) = %v, want an error mentioning %q", tc.spec, got, tc.want)
			continue
		}
		if !strings.Contains(err.Error(), tc.want) {
			t.Errorf("ParseSeeds(%q) error %q does not mention %q", tc.spec, err, tc.want)
		}
	}
}
