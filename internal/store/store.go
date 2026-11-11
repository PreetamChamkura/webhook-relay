// Package store is the Postgres data layer (pgx).
package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/PreetamChamkura/webhook-relay/migrations"
)

var ErrNotFound = errors.New("not found")

type Subscription struct {
	ID              string    `json:"id"`
	URL             string    `json:"url"`
	Secret          string    `json:"secret,omitempty"`
	EventTypes      []string  `json:"event_types"`
	RateLimitPerSec int       `json:"rate_limit_per_sec"`
	Active          bool      `json:"active"`
	CreatedAt       time.Time `json:"created_at"`
}

type Event struct {
	ID             string          `json:"id"`
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload"`
	IdempotencyKey string          `json:"-"`
	CreatedAt      time.Time       `json:"created_at"`
}

type Attempt struct {
	ID             int64     `json:"id"`
	EventID        string    `json:"event_id"`
	SubscriptionID string    `json:"subscription_id"`
	Attempt        int       `json:"attempt"`
	Status         string    `json:"status"`
	ResponseCode   int       `json:"response_code,omitempty"`
	Error          string    `json:"error,omitempty"`
	DurationMS     int       `json:"duration_ms"`
	CreatedAt      time.Time `json:"created_at"`
}

type Store struct{ db *pgxpool.Pool }

func New(db *pgxpool.Pool) *Store { return &Store{db: db} }

func (s *Store) Ping(ctx context.Context) error { return s.db.Ping(ctx) }

// Migrate applies the embedded migrations. url is a postgres:// URL.
func Migrate(url string) error {
	src, err := iofs.New(migrations.FS, ".")
	if err != nil {
		return err
	}
	m, err := migrate.NewWithSourceInstance("iofs", src, "pgx5"+url[len("postgres"):])
	if err != nil {
		return err
	}
	defer func() { _, _ = m.Close() }()
	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return err
	}
	return nil
}

func (s *Store) CreateSubscription(ctx context.Context, sub Subscription) (Subscription, error) {
	err := s.db.QueryRow(ctx, `
		INSERT INTO subscriptions (url, secret, event_types, rate_limit_per_sec)
		VALUES ($1, $2, $3, $4)
		RETURNING id, active, created_at`,
		sub.URL, sub.Secret, sub.EventTypes, sub.RateLimitPerSec,
	).Scan(&sub.ID, &sub.Active, &sub.CreatedAt)
	return sub, err
}

const subCols = `id, url, secret, event_types, rate_limit_per_sec, active, created_at`

func scanSub(row pgx.Row) (Subscription, error) {
	var s Subscription
	err := row.Scan(&s.ID, &s.URL, &s.Secret, &s.EventTypes, &s.RateLimitPerSec, &s.Active, &s.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return s, ErrNotFound
	}
	return s, err
}

func (s *Store) GetSubscription(ctx context.Context, id string) (Subscription, error) {
	return scanSub(s.db.QueryRow(ctx, `SELECT `+subCols+` FROM subscriptions WHERE id = $1`, id))
}

// MatchingSubscriptions returns active subscriptions for eventType (or wildcard '*').
func (s *Store) MatchingSubscriptions(ctx context.Context, eventType string) ([]Subscription, error) {
	rows, err := s.db.Query(ctx, `SELECT `+subCols+` FROM subscriptions
		WHERE active AND event_types && ARRAY[$1, '*']`, eventType)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Subscription
	for rows.Next() {
		sub, err := scanSub(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, sub)
	}
	return out, rows.Err()
}

// InsertEvent stores ev. If its idempotency key already exists it returns the
// existing event's ID and created=false.
func (s *Store) InsertEvent(ctx context.Context, ev Event) (id string, created bool, err error) {
	var key *string
	if ev.IdempotencyKey != "" {
		key = &ev.IdempotencyKey
	}
	err = s.db.QueryRow(ctx, `
		INSERT INTO events (id, event_type, payload, idempotency_key, created_at)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
		RETURNING id`, ev.ID, ev.Type, ev.Payload, key, ev.CreatedAt).Scan(&id)
	if err == nil {
		return id, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return "", false, err
	}
	err = s.db.QueryRow(ctx, `SELECT id FROM events WHERE idempotency_key = $1`, key).Scan(&id)
	return id, false, err
}

func (s *Store) GetEvent(ctx context.Context, id string) (Event, error) {
	var ev Event
	err := s.db.QueryRow(ctx, `SELECT id, event_type, payload, created_at FROM events WHERE id = $1`, id).
		Scan(&ev.ID, &ev.Type, &ev.Payload, &ev.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return ev, ErrNotFound
	}
	return ev, err
}

func (s *Store) AlreadyDelivered(ctx context.Context, eventID, subID string) (bool, error) {
	var ok bool
	err := s.db.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM delivery_attempts
		WHERE event_id = $1 AND subscription_id = $2 AND status = 'succeeded')`, eventID, subID).Scan(&ok)
	return ok, err
}

// RecordAttempt logs an attempt. A second 'succeeded' row for the same pair
// (two workers raced) violates the unique index and is reported as dup=true.
func (s *Store) RecordAttempt(ctx context.Context, a Attempt) (dup bool, err error) {
	_, err = s.db.Exec(ctx, `
		INSERT INTO delivery_attempts (event_id, subscription_id, attempt, status, response_code, error, duration_ms)
		VALUES ($1, $2, $3, $4, NULLIF($5, 0), NULLIF($6, ''), $7)`,
		a.EventID, a.SubscriptionID, a.Attempt, a.Status, a.ResponseCode, a.Error, a.DurationMS)
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return true, nil
	}
	return false, err
}

func (s *Store) ListDeliveries(ctx context.Context, subID string, limit int) ([]Attempt, error) {
	rows, err := s.db.Query(ctx, `
		SELECT id, event_id, subscription_id, attempt, status, COALESCE(response_code, 0), COALESCE(error, ''), duration_ms, created_at
		FROM delivery_attempts WHERE subscription_id = $1
		ORDER BY created_at DESC LIMIT $2`, subID, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (Attempt, error) {
		var a Attempt
		err := r.Scan(&a.ID, &a.EventID, &a.SubscriptionID, &a.Attempt, &a.Status, &a.ResponseCode, &a.Error, &a.DurationMS, &a.CreatedAt)
		return a, err
	})
}

// DeadLetters returns event IDs that died for subID and were never delivered later.
func (s *Store) DeadLetters(ctx context.Context, subID string) ([]string, error) {
	rows, err := s.db.Query(ctx, `
		SELECT DISTINCT d.event_id FROM delivery_attempts d
		WHERE d.subscription_id = $1 AND d.status = 'dead'
		  AND NOT EXISTS (SELECT 1 FROM delivery_attempts s
		                  WHERE s.event_id = d.event_id AND s.subscription_id = d.subscription_id AND s.status = 'succeeded')`, subID)
	if err != nil {
		return nil, err
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, fmt.Errorf("dead letters: %w", err)
	}
	return ids, nil
}
