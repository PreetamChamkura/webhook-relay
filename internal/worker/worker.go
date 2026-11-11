// Package worker delivers events to subscriber endpoints.
package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/PreetamChamkura/webhook-relay/internal/backoff"
	"github.com/PreetamChamkura/webhook-relay/internal/metrics"
	"github.com/PreetamChamkura/webhook-relay/internal/queue"
	"github.com/PreetamChamkura/webhook-relay/internal/signing"
	"github.com/PreetamChamkura/webhook-relay/internal/store"
)

type Store interface {
	GetEvent(ctx context.Context, id string) (store.Event, error)
	GetSubscription(ctx context.Context, id string) (store.Subscription, error)
	MatchingSubscriptions(ctx context.Context, eventType string) ([]store.Subscription, error)
	AlreadyDelivered(ctx context.Context, eventID, subID string) (bool, error)
	RecordAttempt(ctx context.Context, a store.Attempt) (dup bool, err error)
}

type Limiter interface {
	Allow(ctx context.Context, key string, rate, burst int) (bool, error)
}

type Worker struct {
	Store       Store
	Limiter     Limiter
	DLQ         queue.Producer
	HTTP        *http.Client
	MaxAttempts int
	Backoff     backoff.Policy
	Log         *slog.Logger
	Now         func() time.Time
}

// ErrAborted means shutdown interrupted retries; the message must be redelivered.
var ErrAborted = errors.New("delivery aborted by shutdown")

// Envelope is the JSON body POSTed to subscribers.
type Envelope struct {
	ID        string          `json:"id"`
	Type      string          `json:"type"`
	CreatedAt time.Time       `json:"created_at"`
	Data      json.RawMessage `json:"data"`
}

// Handle is a queue.Handler: fan an event out to its subscriptions.
func (w *Worker) Handle(stop, work context.Context, m queue.Message) error {
	ev, err := w.Store.GetEvent(work, m.EventID)
	if errors.Is(err, store.ErrNotFound) {
		w.Log.Error("event missing, dropping", "event_id", m.EventID)
		return nil
	}
	if err != nil {
		return err
	}
	var subs []store.Subscription
	if m.SubscriptionID != "" {
		sub, err := w.Store.GetSubscription(work, m.SubscriptionID)
		if err != nil {
			return err
		}
		subs = []store.Subscription{sub}
	} else if subs, err = w.Store.MatchingSubscriptions(work, ev.Type); err != nil {
		return err
	}

	var wg sync.WaitGroup
	errs := make([]error, len(subs))
	for i, sub := range subs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = w.deliver(stop, work, ev, sub)
		}()
	}
	wg.Wait()
	return errors.Join(errs...)
}

func (w *Worker) now() time.Time {
	if w.Now != nil {
		return w.Now()
	}
	return time.Now()
}

func (w *Worker) deliver(stop, work context.Context, ev store.Event, sub store.Subscription) error {
	log := w.Log.With("event_id", ev.ID, "subscription_id", sub.ID)
	// Redelivered Kafka message (crash after delivery, before commit): skip.
	if done, err := w.Store.AlreadyDelivered(work, ev.ID, sub.ID); err != nil {
		return err
	} else if done {
		metrics.Deliveries.WithLabelValues("duplicate").Inc()
		return nil
	}

	body, _ := json.Marshal(Envelope{ID: ev.ID, Type: ev.Type, CreatedAt: ev.CreatedAt, Data: ev.Payload})
	for attempt := 1; ; attempt++ {
		if err := w.waitForToken(stop, sub); err != nil {
			return err
		}
		start := w.now()
		code, err := w.post(work, sub, ev.ID, body)
		dur := w.now().Sub(start)
		metrics.DeliveryLatency.Observe(dur.Seconds())

		a := store.Attempt{EventID: ev.ID, SubscriptionID: sub.ID, Attempt: attempt, ResponseCode: code, DurationMS: int(dur.Milliseconds())}
		if err == nil {
			a.Status = "succeeded"
			dup, rerr := w.Store.RecordAttempt(work, a)
			if dup {
				log.Warn("delivered twice (concurrent worker won the race)")
			}
			metrics.Deliveries.WithLabelValues("succeeded").Inc()
			metrics.EndToEnd.Observe(w.now().Sub(ev.CreatedAt).Seconds())
			return rerr
		}
		if work.Err() != nil { // hard deadline hit mid-request; leave for redelivery
			return ErrAborted
		}

		a.Error = err.Error()
		if attempt >= w.MaxAttempts {
			a.Status = "dead"
			metrics.Deliveries.WithLabelValues("dead").Inc()
			metrics.DeadLettered.Inc()
			log.Warn("retries exhausted, dead-lettering", "attempts", attempt, "err", err)
			if err := w.DLQ.DeadLetter(work, queue.Message{EventID: ev.ID, SubscriptionID: sub.ID, Reason: a.Error}); err != nil {
				return err
			}
			_, rerr := w.Store.RecordAttempt(work, a)
			return rerr
		}

		a.Status = "failed"
		metrics.Deliveries.WithLabelValues("failed").Inc()
		metrics.Retries.Inc()
		if _, err := w.Store.RecordAttempt(work, a); err != nil {
			return err
		}
		delay := w.Backoff.Delay(attempt)
		log.Info("delivery failed, backing off", "attempt", attempt, "delay", delay, "err", err)
		select {
		case <-time.After(delay):
		case <-stop.Done():
			// Don't hold up shutdown waiting out a backoff; Kafka redelivers.
			return ErrAborted
		}
	}
}

func (w *Worker) waitForToken(stop context.Context, sub store.Subscription) error {
	for {
		ok, err := w.Limiter.Allow(stop, sub.ID, sub.RateLimitPerSec, sub.RateLimitPerSec)
		if err != nil {
			if stop.Err() != nil {
				return ErrAborted
			}
			return err
		}
		if ok {
			return nil
		}
		metrics.RateLimited.Inc()
		select {
		case <-time.After(time.Second / time.Duration(max(sub.RateLimitPerSec, 1))):
		case <-stop.Done():
			return ErrAborted
		}
	}
}

func (w *Worker) post(ctx context.Context, sub store.Subscription, eventID string, body []byte) (int, error) {
	ts := w.now().Unix()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, sub.URL, bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Webhook-Id", eventID)
	req.Header.Set("Webhook-Timestamp", strconv.FormatInt(ts, 10))
	req.Header.Set("Webhook-Signature", signing.Sign(sub.Secret, ts, body))
	resp, err := w.HTTP.Do(req)
	if err != nil {
		return 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return resp.StatusCode, fmt.Errorf("subscriber returned %d", resp.StatusCode)
	}
	return resp.StatusCode, nil
}
