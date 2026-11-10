// Package backoff computes retry delays: exponential growth capped at Max,
// with "full jitter" (uniform in [0, cap]) so retries from many workers
// hitting the same failing subscriber don't synchronise.
package backoff

import (
	"math/rand/v2"
	"time"
)

type Policy struct {
	Base time.Duration
	Max  time.Duration
	// Jitter returns a value in [0, n). Defaults to math/rand/v2.Int64N.
	Jitter func(n int64) int64
}

// Delay returns the wait before retry number attempt (1-based: the wait after the first failure is Delay(1)).
func (p Policy) Delay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	ceil := p.Base
	for i := 1; i < attempt && ceil < p.Max; i++ {
		ceil *= 2
	}
	if ceil > p.Max {
		ceil = p.Max
	}
	if ceil <= 0 {
		return 0
	}
	j := p.Jitter
	if j == nil {
		j = rand.Int64N
	}
	return time.Duration(j(int64(ceil) + 1))
}
