package backoff

import (
	"testing"
	"time"
)

func TestDelayCeiling(t *testing.T) {
	max := func(n int64) int64 { return n - 1 } // always pick the top of the range
	p := Policy{Base: 100 * time.Millisecond, Max: 2 * time.Second, Jitter: max}
	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{0, 100 * time.Millisecond},
		{1, 100 * time.Millisecond},
		{2, 200 * time.Millisecond},
		{3, 400 * time.Millisecond},
		{5, 1600 * time.Millisecond},
		{6, 2 * time.Second},
		{50, 2 * time.Second},
	}
	for _, tc := range tests {
		if got := p.Delay(tc.attempt); got != tc.want {
			t.Errorf("Delay(%d) = %v, want %v", tc.attempt, got, tc.want)
		}
	}
}

func TestDelayJitterBounds(t *testing.T) {
	p := Policy{Base: 50 * time.Millisecond, Max: time.Second}
	for i := 0; i < 1000; i++ {
		d := p.Delay(4)
		if d < 0 || d > 400*time.Millisecond {
			t.Fatalf("delay %v out of [0, 400ms]", d)
		}
	}
}
