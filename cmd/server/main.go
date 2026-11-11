package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
	
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/yashyadav-4/ware_go/internal/worker"
	"github.com/segmentio/kafka-go"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/yashyadav-4/ware_go/internal/config"
	"github.com/yashyadav-4/ware_go/internal/repository/db"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/yashyadav-4/ware_go/internal/api"
	apiMiddleware "github.com/yashyadav-4/ware_go/internal/api/middleware"
	"github.com/redis/go-redis/v9"
)

func main(){
	logger := slog.New(slog.NewJSONHandler(os.Stdout,nil))
	slog.SetDefault(logger)

	cfg := config.Load()

	ctx , stop := signal.NotifyContext(context.Background() ,os.Interrupt , syscall.SIGTERM)
	defer stop()

	slog.Info("connecting to postgresql pool...")
	poolConfig , err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if(err != nil){
		slog.Error("failed to parse database url", "error", err)
		os.Exit(1)
	}

	poolConfig.MaxConns = 25
	poolConfig.MinConns = 5
	poolConfig.MaxConnIdleTime = 5 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		slog.Error("failed to create db pool", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		slog.Error("failed to ping postgresql", "error", err)
		os.Exit(1)
	}
	slog.Info("connected to postgresql successfully")

	slog.Info("Connecting to redis....")
	rdb := redis.NewClient(&redis.Options{
		Addr: cfg.RedisAddr,
	})
	defer rdb.Close()

	if err := rdb.Ping(ctx).Err(); err != nil {
		slog.Error("failed to ping redis", "error", err)
		os.Exit(1)
	}
	slog.Info("connected to redis successfully")


	queries := db.New(pool)

	//kafka
	kafkaWriter := &kafka.Writer{
		Addr:     kafka.TCP(cfg.KafkaBrokers),
		Balancer: &kafka.LeastBytes{},
	}
	defer kafkaWriter.Close()

	//reaper daemon for reclaiming dead reservations
	reaper := worker.NewReaper(pool, queries, worker.ReaperConfig{
		Interval: 1 * time.Second,
		BatchSize: 100,
	})
	go reaper.Start(ctx)

	//for streaming to kafka
	outboxRelay := worker.NewOutboxRelay(pool, queries, kafkaWriter, worker.OutboxRelayConfig{
		Interval:  500 * time.Millisecond,
		BatchSize: 100,
		Topic:     "orders.events",
	})
	go outboxRelay.Start(ctx)

	//simulates third party consumer after order placement
	kafkaReader := worker.NewKafkaReader(worker.ConsumerConfig{
		Brokers: []string{cfg.KafkaBrokers},
		Topic:   "orders.events",
		GroupID: "warego.logistics.dispatcher",
	})
	consumer := worker.NewLogisticsConsumer(kafkaReader)
	go consumer.Start(ctx)

	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Use(middleware.Timeout(60 * time.Second))
	r.Use(apiMiddleware.Semaphore(100 ,100 * time.Millisecond))
	r.Mount("/debug", middleware.Profiler())

	r.Get("/healthz" , func (w http.ResponseWriter , r *http.Request){
		if err := pool.Ping(r.Context()); err != nil {
			http.Error(w , `{"status":"database unreachable"}`, http.StatusServiceUnavailable)
			return
		}

		if err := rdb.Ping(r.Context()).Err(); err != nil {
			http.Error(w, `{"status":"redis unreachable"}`, http.StatusServiceUnavailable)
			return
		}

		w.Header().Set("Content-Type" , "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok"}`))
	})

	r.Handle("/metrics", promhttp.Handler())

	api.RegisterRoutes(r, pool , queries , rdb)

	server := &http.Server{
		Addr:         fmt.Sprintf(":%s", cfg.Port),
		Handler:      r,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	go func(){
		slog.Info("http server listening", "port", cfg.Port)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("http server failed", "error", err)
			os.Exit(1)
		}
	}()


	<-ctx.Done()
	slog.Info("shutting down server gracefully...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		slog.Error("server forced to shutdown", "error", err)
	}
	slog.Info("server shutdown complete")

}