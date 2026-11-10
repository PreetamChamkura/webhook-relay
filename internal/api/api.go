// Package api is the REST API (chi).
package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/PreetamChamkura/webhook-relay/internal/queue"
	"github.com/PreetamChamkura/webhook-relay/internal/service"
	"github.com/PreetamChamkura/webhook-relay/internal/store"
)

type API struct {
	Store *store.Store
	Pub   *service.Publisher
	Queue queue.Producer
	Log   *slog.Logger
	Ready func(context.Context) error
}

func (a *API) Router() http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.RequestID, a.logRequests, middleware.Recoverer)
	r.Get("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	r.Get("/readyz", a.ready)
	r.Handle("/metrics", promhttp.Handler())
	r.Route("/v1", func(r chi.Router) {
		r.Post("/subscriptions", a.createSubscription)
		r.Get("/subscriptions/{id}", a.getSubscription)
		r.Get("/subscriptions/{id}/deliveries", a.listDeliveries)
		r.Post("/subscriptions/{id}/replay", a.replay)
		r.Post("/events", a.publish)
	})
	return r
}

func (a *API) logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
		start := time.Now()
		next.ServeHTTP(ww, r)
		if r.URL.Path == "/metrics" || r.URL.Path == "/healthz" {
			return
		}
		a.Log.Info("http request",
			"request_id", middleware.GetReqID(r.Context()),
			"method", r.Method, "path", r.URL.Path,
			"status", ww.Status(), "duration_ms", time.Since(start).Milliseconds())
	})
}

func (a *API) ready(w http.ResponseWriter, r *http.Request) {
	if err := a.Ready(r.Context()); err != nil {
		writeErr(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	w.WriteHeader(http.StatusOK)
}

type createSubReq struct {
	URL             string   `json:"url"`
	EventTypes      []string `json:"event_types"`
	RateLimitPerSec int      `json:"rate_limit_per_sec"`
}

func (a *API) createSubscription(w http.ResponseWriter, r *http.Request) {
	var req createSubReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid JSON")
		return
	}
	if u, err := url.Parse(req.URL); err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		writeErr(w, http.StatusBadRequest, "url must be an absolute http(s) URL")
		return
	}
	if len(req.EventTypes) == 0 {
		req.EventTypes = []string{"*"}
	}
	if req.RateLimitPerSec <= 0 {
		req.RateLimitPerSec = 50
	}
	secret := make([]byte, 24)
	_, _ = rand.Read(secret)
	sub, err := a.Store.CreateSubscription(r.Context(), store.Subscription{
		URL: req.URL, Secret: "whsec_" + hex.EncodeToString(secret), EventTypes: req.EventTypes, RateLimitPerSec: req.RateLimitPerSec,
	})
	if err != nil {
		a.internal(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, sub) // secret is shown once, here
}

func (a *API) getSubscription(w http.ResponseWriter, r *http.Request) {
	sub, err := a.Store.GetSubscription(r.Context(), chi.URLParam(r, "id"))
	if errors.Is(err, store.ErrNotFound) {
		writeErr(w, http.StatusNotFound, "subscription not found")
		return
	}
	if err != nil {
		a.internal(w, r, err)
		return
	}
	sub.Secret = ""
	writeJSON(w, http.StatusOK, sub)
}

type publishReq struct {
	EventType string          `json:"event_type"`
	Payload   json.RawMessage `json:"payload"`
}

func (a *API) publish(w http.ResponseWriter, r *http.Request) {
	var req publishReq
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 256<<10)).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid JSON")
		return
	}
	id, dup, err := a.Pub.Publish(r.Context(), "http", req.EventType, req.Payload, r.Header.Get("Idempotency-Key"))
	if errors.Is(err, service.ErrInvalid) {
		writeErr(w, http.StatusBadRequest, "event_type and a JSON payload are required")
		return
	}
	if err != nil {
		a.internal(w, r, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"id": id, "duplicate": dup})
}

func (a *API) listDeliveries(w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	out, err := a.Store.ListDeliveries(r.Context(), chi.URLParam(r, "id"), limit)
	if err != nil {
		a.internal(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"deliveries": out})
}

// replay re-enqueues every dead-lettered event for the subscription.
func (a *API) replay(w http.ResponseWriter, r *http.Request) {
	subID := chi.URLParam(r, "id")
	if _, err := a.Store.GetSubscription(r.Context(), subID); errors.Is(err, store.ErrNotFound) {
		writeErr(w, http.StatusNotFound, "subscription not found")
		return
	}
	ids, err := a.Store.DeadLetters(r.Context(), subID)
	if err != nil {
		a.internal(w, r, err)
		return
	}
	for _, id := range ids {
		if err := a.Queue.Enqueue(r.Context(), queue.Message{EventID: id, SubscriptionID: subID}); err != nil {
			a.internal(w, r, err)
			return
		}
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"replayed": len(ids)})
}

func (a *API) internal(w http.ResponseWriter, r *http.Request, err error) {
	a.Log.Error("request failed", "request_id", middleware.GetReqID(r.Context()), "err", err)
	writeErr(w, http.StatusInternalServerError, "internal error")
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}
