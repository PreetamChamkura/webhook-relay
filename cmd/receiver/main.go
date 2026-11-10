// Command receiver is a test subscriber: it verifies signatures, counts unique and
// duplicate deliveries, records end-to-end latency, and can be told to fail
// (FAIL_RATE=0.3, or POST /toggle to go fully down/up) to exercise retries and the DLQ.
package main

import (
	"encoding/json"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/PreetamChamkura/webhook-relay/internal/signing"
)

var (
	received = promauto.NewCounterVec(prometheus.CounterOpts{Name: "receiver_webhooks_total"}, []string{"result"})
	e2e      = promauto.NewHistogram(prometheus.HistogramOpts{Name: "receiver_end_to_end_seconds",
		Buckets: prometheus.ExponentialBuckets(0.005, 2, 14)})
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	secret := os.Getenv("WEBHOOK_SECRET")
	failRate, _ := strconv.ParseFloat(os.Getenv("FAIL_RATE"), 64)
	var down atomic.Bool
	var seen sync.Map

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("POST /toggle", func(w http.ResponseWriter, _ *http.Request) {
		v := !down.Load()
		down.Store(v)
		_, _ = w.Write([]byte(`{"down":` + strconv.FormatBool(v) + "}"))
	})
	mux.HandleFunc("POST /hook", func(w http.ResponseWriter, r *http.Request) {
		if down.Load() || rand.Float64() < failRate {
			received.WithLabelValues("rejected").Inc()
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		body, _ := io.ReadAll(r.Body)
		if secret != "" {
			ts, _ := strconv.ParseInt(r.Header.Get("Webhook-Timestamp"), 10, 64)
			if !signing.Verify(secret, r.Header.Get("Webhook-Signature"), ts, body, time.Now(), 5*time.Minute) {
				received.WithLabelValues("bad_signature").Inc()
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
		}
		if _, dup := seen.LoadOrStore(r.Header.Get("Webhook-Id"), struct{}{}); dup {
			received.WithLabelValues("duplicate").Inc()
		} else {
			received.WithLabelValues("unique").Inc()
		}
		var env struct {
			CreatedAt time.Time `json:"created_at"`
		}
		if json.Unmarshal(body, &env) == nil {
			e2e.Observe(time.Since(env.CreatedAt).Seconds())
		}
	})
	addr := os.Getenv("ADDR")
	if addr == "" {
		addr = ":8090"
	}
	log.Info("receiver listening", "addr", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Error("listen", "err", err)
		os.Exit(1)
	}
}
