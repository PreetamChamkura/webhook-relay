package signing

import (
	"testing"
	"time"
)

func TestVerify(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	body := []byte(`{"hello":"world"}`)
	good := Sign("s3cret", now.Unix(), body)

	tests := []struct {
		name   string
		secret string
		header string
		ts     int64
		body   []byte
		want   bool
	}{
		{"valid", "s3cret", good, now.Unix(), body, true},
		{"wrong secret", "other", good, now.Unix(), body, false},
		{"tampered body", "s3cret", good, now.Unix(), []byte(`{"hello":"mars"}`), false},
		{"missing prefix", "s3cret", good[len(Prefix):], now.Unix(), body, false},
		{"stale timestamp", "s3cret", Sign("s3cret", now.Unix()-600, body), now.Unix() - 600, body, false},
		{"timestamp swapped", "s3cret", good, now.Unix() + 1, body, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := Verify(tc.secret, tc.header, tc.ts, tc.body, now, 5*time.Minute); got != tc.want {
				t.Fatalf("Verify = %v, want %v", got, tc.want)
			}
		})
	}
}
