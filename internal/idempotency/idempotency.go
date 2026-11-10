// Package idempotency de-duplicates publishes with Redis SET NX + TTL.
//
// The first publish with a key claims it and stores its event ID; any repeat
// within the TTL gets the original event ID back instead of creating a new event.
// Postgres has a unique index on events.idempotency_key as a durable backstop
// for repeats that arrive after the TTL or if Redis loses data.
package idempotency

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

type Store struct {
	rdb redis.Cmdable
	ttl time.Duration
}

func New(rdb redis.Cmdable, ttl time.Duration) *Store { return &Store{rdb: rdb, ttl: ttl} }

// Reserve claims key for eventID. If the key was already claimed, it returns the
// event ID that owns it and fresh=false.
func (s *Store) Reserve(ctx context.Context, key, eventID string) (owner string, fresh bool, err error) {
	ok, err := s.rdb.SetNX(ctx, "idem:"+key, eventID, s.ttl).Result()
	if err != nil {
		return "", false, err
	}
	if ok {
		return eventID, true, nil
	}
	owner, err = s.rdb.Get(ctx, "idem:"+key).Result()
	if errors.Is(err, redis.Nil) { // expired between SETNX and GET: try once more
		return s.Reserve(ctx, key, eventID)
	}
	return owner, false, err
}

// Release frees a key whose publish failed so the client can retry.
func (s *Store) Release(ctx context.Context, key string) error {
	return s.rdb.Del(ctx, "idem:"+key).Err()
}
