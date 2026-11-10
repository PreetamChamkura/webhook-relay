// Package queue wraps Kafka (franz-go): producing delivery jobs and a
// consumer-group loop that commits offsets only after a batch is handled.
package queue

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/PreetamChamkura/webhook-relay/internal/metrics"
)

// Message is a delivery job. With SubscriptionID empty the worker fans out to
// every matching subscription; with it set (replay) only that one is tried.
type Message struct {
	EventID        string `json:"event_id"`
	SubscriptionID string `json:"subscription_id,omitempty"`
	Reason         string `json:"reason,omitempty"` // set on DLQ messages
}

type Producer interface {
	Enqueue(ctx context.Context, m Message) error
	DeadLetter(ctx context.Context, m Message) error
}

type Kafka struct {
	cl       *kgo.Client
	topic    string
	dlqTopic string
}

func NewProducer(brokers []string, topic, dlqTopic string) (*Kafka, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.AllowAutoTopicCreation(),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerLinger(time.Millisecond),
	)
	if err != nil {
		return nil, err
	}
	return &Kafka{cl: cl, topic: topic, dlqTopic: dlqTopic}, nil
}

func (k *Kafka) produce(ctx context.Context, topic string, m Message) error {
	b, _ := json.Marshal(m)
	// Key by event ID so retries/replays of one event stay on one partition.
	return k.cl.ProduceSync(ctx, &kgo.Record{Topic: topic, Key: []byte(m.EventID), Value: b}).FirstErr()
}

func (k *Kafka) Enqueue(ctx context.Context, m Message) error { return k.produce(ctx, k.topic, m) }
func (k *Kafka) DeadLetter(ctx context.Context, m Message) error {
	return k.produce(ctx, k.dlqTopic, m)
}
func (k *Kafka) Ping(ctx context.Context) error { return k.cl.Ping(ctx) }
func (k *Kafka) Close()                         { k.cl.Close() }

// Handler processes one message. stop is cancelled when shutdown begins; work
// is cancelled only when the drain deadline passes. Returning an error means
// "not handled" and prevents the batch's offsets from being committed.
type Handler func(stop, work context.Context, m Message) error

type Consumer struct {
	cl          *kgo.Client
	concurrency int
	log         *slog.Logger
}

func NewConsumer(brokers []string, topic, group string, concurrency int, log *slog.Logger) (*Consumer, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.AllowAutoTopicCreation(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		return nil, err
	}
	return &Consumer{cl: cl, concurrency: concurrency, log: log}, nil
}

// Run polls until stop is cancelled. Each polled batch is fanned out to a pool of
// `concurrency` goroutines; offsets are committed only if every record succeeded.
// On shutdown the in-flight batch is drained (bounded by work) and, if any
// record was abandoned, nothing is committed so Kafka redelivers it.
func (c *Consumer) Run(stop, work context.Context, h Handler) error {
	defer c.cl.Close()
	for {
		fetches := c.cl.PollFetches(stop)
		if stop.Err() != nil {
			c.cl.AllowRebalance()
			return nil
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				c.log.Error("kafka fetch", "topic", e.Topic, "partition", e.Partition, "err", e.Err)
			}
		}
		recs := fetches.Records()
		if len(recs) > 0 && c.process(stop, work, recs, h) {
			if err := c.cl.CommitRecords(work, recs...); err != nil {
				c.log.Error("commit offsets", "err", err)
			}
		}
		c.cl.AllowRebalance()
	}
}

func (c *Consumer) process(stop, work context.Context, recs []*kgo.Record, h Handler) bool {
	jobs := make(chan *kgo.Record)
	var wg sync.WaitGroup
	var mu sync.Mutex
	ok := true
	n := min(c.concurrency, len(recs))
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for r := range jobs {
				if err := c.handleRecord(stop, work, r, h); err != nil {
					mu.Lock()
					ok = false
					mu.Unlock()
				}
			}
		}()
	}
	for _, r := range recs {
		jobs <- r
	}
	close(jobs)
	wg.Wait()
	return ok
}

func (c *Consumer) handleRecord(stop, work context.Context, r *kgo.Record, h Handler) error {
	metrics.QueueLag.Observe(time.Since(r.Timestamp).Seconds())
	var m Message
	if err := json.Unmarshal(r.Value, &m); err != nil {
		c.log.Error("dropping malformed message", "offset", r.Offset, "err", err)
		return nil
	}
	// Infrastructure errors (DB/Redis down) are retried here until they clear or
	// shutdown begins; giving up would mean committing past an unhandled record.
	delay := 100 * time.Millisecond
	for {
		err := h(stop, work, m)
		if err == nil {
			return nil
		}
		if stop.Err() != nil || errors.Is(err, context.Canceled) {
			return err
		}
		c.log.Warn("handler failed, retrying", "event_id", m.EventID, "err", err)
		select {
		case <-time.After(delay):
		case <-stop.Done():
			return err
		}
		delay = min(delay*2, 5*time.Second)
	}
}
