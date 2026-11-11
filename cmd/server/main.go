// Command server runs the REST API, the gRPC API and the delivery workers in one
// process. In Kubernetes they scale together; the HPA scales on CPU.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	webhookv1 "github.com/PreetamChamkura/webhook-relay/gen/webhook/v1"
	"github.com/PreetamChamkura/webhook-relay/internal/api"
	"github.com/PreetamChamkura/webhook-relay/internal/backoff"
	"github.com/PreetamChamkura/webhook-relay/internal/config"
	"github.com/PreetamChamkura/webhook-relay/internal/grpcapi"
	"github.com/PreetamChamkura/webhook-relay/internal/idempotency"
	"github.com/PreetamChamkura/webhook-relay/internal/queue"
	"github.com/PreetamChamkura/webhook-relay/internal/ratelimit"
	"github.com/PreetamChamkura/webhook-relay/internal/service"
	"github.com/PreetamChamkura/webhook-relay/internal/store"
	"github.com/PreetamChamkura/webhook-relay/internal/worker"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	cfg := config.Load()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if cfg.RunMigrations {
		if err := store.Migrate(cfg.DatabaseURL); err != nil {
			return err
		}
	}
	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	ropts, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		return err
	}
	rdb := redis.NewClient(ropts)
	defer func() { _ = rdb.Close() }()
	producer, err := queue.NewProducer(cfg.KafkaBrokers, cfg.Topic, cfg.DLQTopic)
	if err != nil {
		return err
	}
	defer producer.Close()
	consumer, err := queue.NewConsumer(cfg.KafkaBrokers, cfg.Topic, cfg.ConsumerGroup, cfg.Workers, log)
	if err != nil {
		return err
	}

	st := store.New(pool)
	pub := &service.Publisher{Store: st, Idem: idempotency.New(rdb, cfg.IdempotencyTTL), Queue: producer}
	w := &worker.Worker{
		Store: st, Limiter: ratelimit.New(rdb), DLQ: producer,
		HTTP:        &http.Client{Timeout: cfg.DeliveryTimeout},
		MaxAttempts: cfg.MaxAttempts,
		Backoff:     backoff.Policy{Base: cfg.BackoffBase, Max: cfg.BackoffMax},
		Log:         log,
	}

	a := &api.API{Store: st, Pub: pub, Queue: producer, Log: log, Ready: func(ctx context.Context) error {
		return errors.Join(st.Ping(ctx), rdb.Ping(ctx).Err(), producer.Ping(ctx))
	}}
	httpSrv := &http.Server{Addr: cfg.HTTPAddr, Handler: a.Router(), ReadHeaderTimeout: 5 * time.Second}

	grpcSrv := grpc.NewServer()
	webhookv1.RegisterEventServiceServer(grpcSrv, &grpcapi.Server{Pub: pub})
	hs := health.NewServer()
	healthpb.RegisterHealthServer(grpcSrv, hs)
	lis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		return err
	}

	// work outlives ctx by up to ShutdownTimeout so in-flight HTTP deliveries can finish.
	work, cancelWork := context.WithCancel(context.Background())
	defer cancelWork()

	errc := make(chan error, 3)
	consumerDone := make(chan struct{})
	go func() { errc <- httpSrv.ListenAndServe() }()
	go func() { errc <- grpcSrv.Serve(lis) }()
	go func() { defer close(consumerDone); errc <- consumer.Run(ctx, work, w.Handle) }()
	log.Info("started", "http", cfg.HTTPAddr, "grpc", cfg.GRPCAddr, "workers", cfg.Workers)

	select {
	case <-ctx.Done():
	case err := <-errc:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("component failed", "err", err)
		}
		stop()
	}

	log.Info("shutting down: stop accepting, drain in-flight deliveries", "timeout", cfg.ShutdownTimeout)
	hs.Shutdown()
	deadline := time.AfterFunc(cfg.ShutdownTimeout, cancelWork)
	defer deadline.Stop()
	sctx, scancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer scancel()
	_ = httpSrv.Shutdown(sctx)
	grpcSrv.GracefulStop()
	<-consumerDone
	log.Info("shutdown complete")
	return nil
}
