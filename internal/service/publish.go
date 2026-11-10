// Package service holds the publish path shared by the REST and gRPC APIs.
package service

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"

	"github.com/PreetamChamkura/webhook-relay/internal/idempotency"
	"github.com/PreetamChamkura/webhook-relay/internal/metrics"
	"github.com/PreetamChamkura/webhook-relay/internal/queue"
	"github.com/PreetamChamkura/webhook-relay/internal/store"
)

var ErrInvalid = errors.New("invalid event")

type Publisher struct {
	Store *store.Store
	Idem  *idempotency.Store
	Queue queue.Producer
}

// Publish persists the event, then enqueues it. Order matters: the row exists
// before any worker can see the message. If enqueue fails the client gets an
// error and retries with the same Idempotency-Key, which re-enqueues the
// existing event rather than creating a new one.
func (p *Publisher) Publish(ctx context.Context, transport, eventType string, payload json.RawMessage, key string) (id string, duplicate bool, err error) {
	start := time.Now()
	defer func() {
		metrics.PublishLatency.WithLabelValues(transport).Observe(time.Since(start).Seconds())
		switch {
		case err != nil:
			metrics.Published.WithLabelValues("error").Inc()
		case duplicate:
			metrics.Published.WithLabelValues("duplicate").Inc()
		default:
			metrics.Published.WithLabelValues("created").Inc()
		}
	}()
	if eventType == "" || !json.Valid(payload) {
		return "", false, ErrInvalid
	}
	id = uuid.NewString()
	if key != "" {
		owner, fresh, err := p.Idem.Reserve(ctx, key, id)
		if err != nil {
			return "", false, err
		}
		if !fresh {
			return owner, true, nil
		}
	}
	id, created, err := p.Store.InsertEvent(ctx, store.Event{ID: id, Type: eventType, Payload: payload, IdempotencyKey: key, CreatedAt: time.Now().UTC()})
	if err != nil {
		if key != "" {
			_ = p.Idem.Release(ctx, key)
		}
		return "", false, err
	}
	// !created: Redis forgot the key but Postgres didn't (TTL expiry, or an
	// earlier attempt inserted and then failed to enqueue). Re-enqueue anyway:
	// workers skip pairs that already succeeded, so this can't double-deliver.
	if err := p.Queue.Enqueue(ctx, queue.Message{EventID: id}); err != nil {
		if key != "" {
			_ = p.Idem.Release(ctx, key)
		}
		return "", false, err
	}
	return id, !created, nil
}
