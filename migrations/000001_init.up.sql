CREATE TABLE subscriptions (
    id                 uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    url                text        NOT NULL,
    secret             text        NOT NULL,
    event_types        text[]      NOT NULL DEFAULT '{*}',
    rate_limit_per_sec integer     NOT NULL DEFAULT 50,
    active             boolean     NOT NULL DEFAULT true,
    created_at         timestamptz NOT NULL DEFAULT now()
);
-- Fan-out lookup: "active subscriptions whose event_types overlap {type, '*'}".
-- GIN supports the && array-overlap operator; partial on active keeps it small.
CREATE INDEX subscriptions_event_types_gin ON subscriptions USING gin (event_types) WHERE active;

CREATE TABLE events (
    id              uuid PRIMARY KEY,
    event_type      text        NOT NULL,
    payload         jsonb       NOT NULL,
    idempotency_key text,
    created_at      timestamptz NOT NULL DEFAULT now()
);
-- Durable idempotency backstop behind Redis SET NX (survives TTL expiry / Redis loss).
CREATE UNIQUE INDEX events_idempotency_key_uniq ON events (idempotency_key) WHERE idempotency_key IS NOT NULL;

CREATE TABLE delivery_attempts (
    id              bigserial PRIMARY KEY,
    event_id        uuid        NOT NULL REFERENCES events (id),
    subscription_id uuid        NOT NULL REFERENCES subscriptions (id),
    attempt         integer     NOT NULL,
    status          text        NOT NULL CHECK (status IN ('succeeded', 'failed', 'dead')),
    response_code   integer,
    error           text,
    duration_ms     integer     NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now()
);
-- GET /subscriptions/{id}/deliveries: newest-first scan for one subscriber.
CREATE INDEX delivery_attempts_sub_created ON delivery_attempts (subscription_id, created_at DESC);
-- At most one success per (event, subscription). Doubles as the lookup a worker
-- does before delivering, so a redelivered Kafka message is skipped.
CREATE UNIQUE INDEX delivery_attempts_one_success ON delivery_attempts (event_id, subscription_id) WHERE status = 'succeeded';
-- /replay: find dead letters for a subscriber without scanning its successes.
CREATE INDEX delivery_attempts_dead ON delivery_attempts (subscription_id, event_id) WHERE status = 'dead';
