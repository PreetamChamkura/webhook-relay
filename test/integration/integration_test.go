//go:build integration

// Integration + chaos tests against real Postgres, Redis and Redpanda via testcontainers.
// Run: go test -tags integration -timeout 10m ./test/integration/...
package integration

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"

	"github.com/PreetamChamkura/webhook-relay/internal/backoff"
	"github.com/PreetamChamkura/webhook-relay/internal/idempotency"
	"github.com/PreetamChamkura/webhook-relay/internal/queue"
	"github.com/PreetamChamkura/webhook-relay/internal/ratelimit"
	"github.com/PreetamChamkura/webhook-relay/internal/service"
	"github.com/PreetamChamkura/webhook-relay/internal/signing"
	"github.com/PreetamChamkura/webhook-relay/internal/store"
	"github.com/PreetamChamkura/webhook-relay/internal/worker"
)

type env struct {
	st      *store.Store
	rdb     *redis.Client
	brokers []string
	topic   string
	pub     *service.Publisher
	prod    *queue.Kafka
}

func setup(t *testing.T) *env {
	t.Helper()
	ctx := context.Background()
	pg, err := postgres.Run(ctx, "postgres:16-alpine",
		postgres.WithDatabase("webhook"), postgres.WithUsername("webhook"), postgres.WithPassword("webhook"),
		postgres.BasicWaitStrategies())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pg.Terminate(ctx) })
	dsn, _ := pg.ConnectionString(ctx, "sslmode=disable")
	if err := store.Migrate(dsn); err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	rc, err := tcredis.Run(ctx, "redis:7-alpine")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rc.Terminate(ctx) })
	rurl, _ := rc.ConnectionString(ctx)
	ropts, _ := redis.ParseURL(rurl)
	rdb := redis.NewClient(ropts)

	rp, err := redpanda.Run(ctx, "docker.redpanda.com/redpandadata/redpanda:v24.2.4", redpanda.WithAutoCreateTopics())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rp.Terminate(ctx) })
	broker, _ := rp.KafkaSeedBroker(ctx)

	topic := "events-" + uuid.NewString()[:8]
	prod, err := queue.NewProducer([]string{broker}, topic, topic+".dlq")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(prod.Close)
	st := store.New(pool)
	return &env{st: st, rdb: rdb, brokers: []string{broker}, topic: topic, prod: prod,
		pub: &service.Publisher{Store: st, Idem: idempotency.New(rdb, time.Hour), Queue: prod}}
}

func (e *env) worker(maxAttempts int) *worker.Worker {
	return &worker.Worker{Store: e.st, Limiter: ratelimit.New(e.rdb), DLQ: e.prod,
		HTTP: &http.Client{Timeout: 5 * time.Second}, MaxAttempts: maxAttempts,
		Backoff: backoff.Policy{Base: 20 * time.Millisecond, Max: 100 * time.Millisecond},
		Log:     slog.New(slog.DiscardHandler)}
}

// startConsumer runs a consumer; stop() begins graceful shutdown, kill() cancels the hard ctx too.
func (e *env) startConsumer(t *testing.T, w *worker.Worker) (stop, kill func(), done <-chan struct{}) {
	c, err := queue.NewConsumer(e.brokers, e.topic, "g-"+e.topic, 16, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	stopCtx, cancelStop := context.WithCancel(context.Background())
	workCtx, cancelWork := context.WithCancel(context.Background())
	d := make(chan struct{})
	go func() { defer close(d); _ = c.Run(stopCtx, workCtx, w.Handle) }()
	return cancelStop, func() { cancelWork(); cancelStop() }, d
}

type receiver struct {
	srv    *httptest.Server
	mu     sync.Mutex
	counts map[string]int
	down   atomic.Bool
	delay  atomic.Int64 // ns
	badSig atomic.Int32
}

func newReceiver(t *testing.T, secret *string) *receiver {
	r := &receiver{counts: map[string]int{}}
	r.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if r.down.Load() {
			w.WriteHeader(503)
			return
		}
		time.Sleep(time.Duration(r.delay.Load()))
		body, _ := io.ReadAll(req.Body)
		ts, _ := strconv.ParseInt(req.Header.Get("Webhook-Timestamp"), 10, 64)
		if !signing.Verify(*secret, req.Header.Get("Webhook-Signature"), ts, body, time.Now(), time.Minute) {
			r.badSig.Add(1)
		}
		r.mu.Lock()
		r.counts[req.Header.Get("Webhook-Id")]++
		r.mu.Unlock()
	}))
	t.Cleanup(r.srv.Close)
	return r
}

func (r *receiver) stats() (unique, dups int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, n := range r.counts {
		unique++
		dups += n - 1
	}
	return
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("condition not met before timeout")
}

func subscribe(t *testing.T, e *env, url string) store.Subscription {
	sub, err := e.st.CreateSubscription(context.Background(), store.Subscription{
		URL: url, Secret: "whsec_test", EventTypes: []string{"order.created"}, RateLimitPerSec: 10000})
	if err != nil {
		t.Fatal(err)
	}
	return sub
}

func TestPublishDeliverIdempotent(t *testing.T) {
	e := setup(t)
	secret := "whsec_test"
	rcv := newReceiver(t, &secret)
	subscribe(t, e, rcv.srv.URL)
	stop, _, done := e.startConsumer(t, e.worker(5))
	defer func() { stop(); <-done }()

	ctx := context.Background()
	payload := json.RawMessage(`{"order":42}`)
	id1, dup1, err := e.pub.Publish(ctx, "test", "order.created", payload, "key-1")
	if err != nil || dup1 {
		t.Fatalf("first publish: %v dup=%v", err, dup1)
	}
	id2, dup2, err := e.pub.Publish(ctx, "test", "order.created", payload, "key-1")
	if err != nil || !dup2 || id2 != id1 {
		t.Fatalf("duplicate publish: id=%s dup=%v err=%v", id2, dup2, err)
	}
	waitFor(t, 30*time.Second, func() bool { u, _ := rcv.stats(); return u == 1 })
	time.Sleep(time.Second)
	if u, d := rcv.stats(); u != 1 || d != 0 {
		t.Fatalf("unique=%d dups=%d, want 1/0", u, d)
	}
	if rcv.badSig.Load() != 0 {
		t.Fatal("signature verification failed")
	}
}

func TestDeadLetterAndReplay(t *testing.T) {
	e := setup(t)
	secret := "whsec_test"
	rcv := newReceiver(t, &secret)
	rcv.down.Store(true)
	sub := subscribe(t, e, rcv.srv.URL)
	stop, _, done := e.startConsumer(t, e.worker(3))
	defer func() { stop(); <-done }()

	ctx := context.Background()
	for i := 0; i < 5; i++ {
		if _, _, err := e.pub.Publish(ctx, "test", "order.created", json.RawMessage(`{}`), ""); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, 30*time.Second, func() bool {
		ids, _ := e.st.DeadLetters(ctx, sub.ID)
		return len(ids) == 5
	})
	rcv.down.Store(false)
	ids, _ := e.st.DeadLetters(ctx, sub.ID)
	for _, id := range ids {
		if err := e.prod.Enqueue(ctx, queue.Message{EventID: id, SubscriptionID: sub.ID}); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, 30*time.Second, func() bool { u, _ := rcv.stats(); return u == 5 })
	if left, _ := e.st.DeadLetters(ctx, sub.ID); len(left) != 0 {
		t.Fatalf("%d dead letters remain after replay", len(left))
	}
}

// TestChaosKillWorkerMidDelivery kills a consumer while deliveries are in flight
// (hard cancel, offsets uncommitted) and starts a fresh one. Every event must be
// delivered (no loss). Duplicates are bounded to the deliveries that completed at
// the subscriber but whose success row was not yet written when the worker died.
func TestChaosKillWorkerMidDelivery(t *testing.T) {
	e := setup(t)
	secret := "whsec_test"
	rcv := newReceiver(t, &secret)
	rcv.delay.Store(int64(300 * time.Millisecond))
	subscribe(t, e, rcv.srv.URL)

	const n = 200
	ctx := context.Background()
	for i := 0; i < n; i++ {
		if _, _, err := e.pub.Publish(ctx, "test", "order.created", json.RawMessage(`{}`), ""); err != nil {
			t.Fatal(err)
		}
	}

	_, kill, done := e.startConsumer(t, e.worker(5))
	waitFor(t, 30*time.Second, func() bool { u, _ := rcv.stats(); return u >= 20 })
	kill() // simulated crash: in-flight requests cancelled, nothing committed for this batch
	<-done
	killedAt, _ := rcv.stats()
	t.Logf("worker killed after %d unique deliveries", killedAt)

	rcv.delay.Store(0)
	stop, _, done2 := e.startConsumer(t, e.worker(5))
	defer func() { stop(); <-done2 }()
	waitFor(t, 60*time.Second, func() bool { u, _ := rcv.stats(); return u == n })
	time.Sleep(2 * time.Second)

	unique, dups := rcv.stats()
	t.Logf("unique=%d duplicates=%d", unique, dups)
	if unique != n {
		t.Fatalf("lost events: unique=%d want %d", unique, n)
	}
	if dups > 16 { // at most one in-flight batch slot per worker goroutine
		t.Fatalf("too many duplicates: %d", dups)
	}
}
