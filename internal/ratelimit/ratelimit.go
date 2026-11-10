// Package ratelimit is a per-key token bucket stored in Redis.
//
// State lives in a hash {t: tokens, ts: last refill ms}; refill + take happens
// atomically in a Lua script so concurrent workers on different pods share one bucket.
package ratelimit

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

var script = redis.NewScript(`
local rate  = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local now   = tonumber(ARGV[3])
local d = redis.call('HMGET', KEYS[1], 't', 'ts')
local tokens = tonumber(d[1])
local ts = tonumber(d[2])
if tokens == nil then tokens = burst; ts = now end
tokens = math.min(burst, tokens + (now - ts) * rate / 1000)
local allowed = 0
if tokens >= 1 then tokens = tokens - 1; allowed = 1 end
redis.call('HSET', KEYS[1], 't', tokens, 'ts', now)
redis.call('PEXPIRE', KEYS[1], math.ceil(burst / rate * 1000) + 1000)
return allowed
`)

type Limiter struct {
	rdb redis.Scripter
	now func() time.Time
}

func New(rdb redis.Scripter) *Limiter { return &Limiter{rdb: rdb, now: time.Now} }

// Allow takes one token from bucket key refilled at rate tokens/sec, holding at most burst.
func (l *Limiter) Allow(ctx context.Context, key string, rate, burst int) (bool, error) {
	if rate <= 0 {
		return true, nil
	}
	if burst < 1 {
		burst = rate
	}
	n, err := script.Run(ctx, l.rdb, []string{"rl:" + key}, rate, burst, l.now().UnixMilli()).Int()
	return n == 1, err
}
