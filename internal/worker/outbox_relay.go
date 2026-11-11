package worker

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
	"github.com/yashyadav-4/ware_go/internal/metrics"
	"github.com/yashyadav-4/ware_go/internal/repository/db"
)

type KafkaMessageWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

type OutboxRelayConfig struct {
	Interval  time.Duration
	BatchSize int32
	Topic     string
}

type OutboxRelay struct {
	pool    *pgxpool.Pool
	queries *db.Queries
	writer  KafkaMessageWriter
	cfg     OutboxRelayConfig
	logger  *slog.Logger
}

func NewOutboxRelay(pool *pgxpool.Pool, queries *db.Queries, writer KafkaMessageWriter, cfg OutboxRelayConfig) *OutboxRelay {
	if cfg.Interval <= 0 {
		cfg.Interval = 500 * time.Millisecond
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 100
	}
	if cfg.Topic == "" {
		cfg.Topic = "orders.events"
	}

	return &OutboxRelay{
		pool:    pool,
		queries: queries,
		writer:  writer,
		cfg:     cfg,
		logger:  slog.Default().With("component", "outbox_relay"),
	}
}

func (r *OutboxRelay) Start(ctx context.Context) {
	r.logger.Info("outbox relay started", "interval", r.cfg.Interval, "batch_size", r.cfg.BatchSize, "topic", r.cfg.Topic)
	ticker := time.NewTicker(r.cfg.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			r.logger.Info("outbox relay stopping...")
			return
		case <-ticker.C:
			//reclaim any stale leases from crashed workers
			if err := r.queries.ReclaimStaleOutboxLeases(ctx); err != nil {
				r.logger.Error("failed to reclaim stale leases", "error", err)
			}

			//bounded drain loop
			const maxBatchesPerTick = 50
			for batch := 0; batch < maxBatchesPerTick; batch++ {
				if ctx.Err() != nil {
					return
				}

				processedCount, err := r.ProcessPendingEvents(ctx)
				if err != nil {
					r.logger.Error("outbox relay cycle failed", "error", err)
					break
				}

				if processedCount > 0 {
					r.logger.Info("published outbox events to kafka", "count", processedCount)
				}

				if int32(processedCount) < r.cfg.BatchSize {
					break
				}
			}
		}
	}
}

func (r *OutboxRelay) ProcessPendingEvents(ctx context.Context) (int, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to begin outbox tx: %w", err)
	}
	defer tx.Rollback(ctx)

	qtx := r.queries.WithTx(tx)

	events, err := qtx.FetchPendingOutboxEvents(ctx, r.cfg.BatchSize)
	if err != nil {
		return 0, fmt.Errorf("failed to fetch pending outbox events: %w", err)
	}

	if len(events) == 0 {
		return 0, nil
	}

	for _, event := range events {
		msg := kafka.Message{
			Topic: r.cfg.Topic,
			Key:   []byte(event.AggregateID),
			Value: event.Payload,
			Headers: []kafka.Header{
				{Key: "event_type", Value: []byte(event.EventType)},
				{Key: "aggregate_type", Value: []byte(event.AggregateType)},
			},
			Time: time.Now(),
		}

		if err := r.writer.WriteMessages(ctx, msg); err != nil {
			metrics.OutboxEventsTotal.WithLabelValues("failed", event.EventType).Inc()
			r.logger.Error("failed to publish event to kafka; incrementing attempts",
				"event_id", event.ID, "error", err)

			if incErr := qtx.IncrementOutboxAttempts(ctx, event.ID); incErr != nil {
				return 0, fmt.Errorf("failed to increment attempts for event %v: %w", event.ID, incErr)
			}
			continue
		}

		if err := qtx.MarkOutboxEventPublished(ctx, event.ID); err != nil {
			return 0, fmt.Errorf("failed to mark event %v published: %w", event.ID, err)
		}
		metrics.OutboxEventsTotal.WithLabelValues("published", event.EventType).Inc()
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("failed to commit outbox transaction: %w", err)
	}

	return len(events), nil
}