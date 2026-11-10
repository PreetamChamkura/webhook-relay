package worker

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/PreetamChamkura/webhook-relay/internal/backoff"
	"github.com/PreetamChamkura/webhook-relay/internal/queue"
	"github.com/PreetamChamkura/webhook-relay/internal/signing"
	"github.com/PreetamChamkura/webhook-relay/internal/store"
)

type fakeStore struct {
	mu       sync.Mutex
	ev       store.Event
	subs     []store.Subscription
	attempts []store.Attempt
}

func (f *fakeStore) GetEvent(context.Context, string) (store.Event, error) { return f.ev, nil }
func (f *fakeStore) GetSubscription(_ context.Context, id string) (store.Subscription, error) {
	for _, s := range f.subs {
		if s.ID == id {
			return s, nil
		}
	}
	return store.Subscription{}, store.ErrNotFound
}
func (f *fakeStore) MatchingSubscriptions(context.Context, string) ([]store.Subscription, error) {
	return f.subs, nil
}
func (f *fakeStore) AlreadyDelivered(_ context.Context, e, s string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, a := range f.attempts {
		if a.EventID == e && a.SubscriptionID == s && a.Status == "succeeded" {
			return true, nil
		}
	}
	return false, nil
}
func (f *fakeStore) RecordAttempt(_ context.Context, a store.Attempt) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.attempts = append(f.attempts, a)
	return false, nil
}
func (f *fakeStore) statuses() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for _, a := range f.attempts {
		out = append(out, a.Status)
	}
	return out
}

type allowAll struct{}

func (allowAll) Allow(context.Context, string, int, int) (bool, error) { return true, nil }

type fakeDLQ struct{ msgs []queue.Message }

func (d *fakeDLQ) Enqueue(context.Context, queue.Message) error { return nil }
func (d *fakeDLQ) DeadLetter(_ context.Context, m queue.Message) error {
	d.msgs = append(d.msgs, m)
	return nil
}

func TestDeliver(t *testing.T) {
	tests := []struct {
		name         string
		failFirst    int // subscriber returns 500 this many times
		wantStatuses []string
		wantDLQ      int
	}{
		{"first try", 0, []string{"succeeded"}, 0},
		{"retry then success", 2, []string{"failed", "failed", "succeeded"}, 0},
		{"exhausted to DLQ", 99, []string{"failed", "failed", "dead"}, 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				ts, _ := strconv.ParseInt(r.Header.Get("Webhook-Timestamp"), 10, 64)
				if !signing.Verify("sec", r.Header.Get("Webhook-Signature"), ts, body, time.Now(), time.Minute) {
					t.Errorf("bad signature")
				}
				if int(calls.Add(1)) <= tc.failFirst {
					w.WriteHeader(500)
				}
			}))
			defer srv.Close()

			fs := &fakeStore{
				ev:   store.Event{ID: "e1", Type: "t", Payload: []byte(`{}`), CreatedAt: time.Now()},
				subs: []store.Subscription{{ID: "s1", URL: srv.URL, Secret: "sec", RateLimitPerSec: 100}},
			}
			dlq := &fakeDLQ{}
			w := &Worker{Store: fs, Limiter: allowAll{}, DLQ: dlq, HTTP: srv.Client(), MaxAttempts: 3,
				Backoff: backoff.Policy{Base: time.Millisecond, Max: 2 * time.Millisecond}, Log: slog.New(slog.DiscardHandler)}

			if err := w.Handle(context.Background(), context.Background(), queue.Message{EventID: "e1"}); err != nil {
				t.Fatal(err)
			}
			got := fs.statuses()
			if len(got) != len(tc.wantStatuses) {
				t.Fatalf("statuses = %v, want %v", got, tc.wantStatuses)
			}
			for i := range got {
				if got[i] != tc.wantStatuses[i] {
					t.Fatalf("statuses = %v, want %v", got, tc.wantStatuses)
				}
			}
			if len(dlq.msgs) != tc.wantDLQ {
				t.Fatalf("dlq = %d, want %d", len(dlq.msgs), tc.wantDLQ)
			}

			// Redelivery of the same Kafka message must not hit the subscriber again.
			before := calls.Load()
			_ = w.Handle(context.Background(), context.Background(), queue.Message{EventID: "e1"})
			if tc.wantDLQ == 0 && calls.Load() != before {
				t.Fatalf("redelivered message re-sent the webhook")
			}
		})
	}
}

func TestShutdownAbortsBackoff(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(503) }))
	defer srv.Close()
	fs := &fakeStore{
		ev:   store.Event{ID: "e1", Type: "t", Payload: []byte(`{}`), CreatedAt: time.Now()},
		subs: []store.Subscription{{ID: "s1", URL: srv.URL, Secret: "x", RateLimitPerSec: 100}},
	}
	w := &Worker{Store: fs, Limiter: allowAll{}, DLQ: &fakeDLQ{}, HTTP: srv.Client(), MaxAttempts: 5,
		Backoff: backoff.Policy{Base: time.Hour, Max: time.Hour, Jitter: func(n int64) int64 { return n - 1 }},
		Log:     slog.New(slog.DiscardHandler)}
	stop, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(50 * time.Millisecond); cancel() }()
	start := time.Now()
	err := w.Handle(stop, context.Background(), queue.Message{EventID: "e1"})
	if err == nil || time.Since(start) > 5*time.Second {
		t.Fatalf("expected prompt ErrAborted, got %v after %v", err, time.Since(start))
	}
}
