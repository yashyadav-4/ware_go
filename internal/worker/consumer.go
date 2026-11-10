package worker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

type KafkaMessageReader interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

type ConsumerConfig struct {
	Brokers []string
	Topic   string
	GroupID string
}

type LogisticsConsumer struct {
	reader        KafkaMessageReader
	logger        *slog.Logger
	processedMu   sync.Mutex
	processedKeys map[string]struct{}
}

func NewLogisticsConsumer(reader KafkaMessageReader) *LogisticsConsumer {
	return &LogisticsConsumer{
		reader:        reader,
		logger:        slog.Default().With("component", "logistics_consumer"),
		processedKeys: make(map[string]struct{}),
	}
}

func NewKafkaReader(cfg ConsumerConfig) *kafka.Reader {
	if cfg.Topic == "" {
		cfg.Topic = "orders.events"
	}
	if cfg.GroupID == "" {
		cfg.GroupID = "warego.logistics.dispatcher"
	}

	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:        cfg.Brokers,
		Topic:          cfg.Topic,
		GroupID:        cfg.GroupID,
		MinBytes:       10e3,
		MaxBytes:       10e6,
		CommitInterval: 1 * time.Second,
	})
}

func (c *LogisticsConsumer) Start(ctx context.Context) {
	c.logger.Info("3PL logistics consumer started; listening for order events...")
	defer c.reader.Close()

	for {
		if ctx.Err() != nil {
			c.logger.Info("logistics consumer stopping...")
			return
		}

		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, io.EOF) {
				return
			}
			c.logger.Error("failed to fetch message from kafka", "error", err)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		orderID := string(msg.Key)
		eventType := ""
		for _, h := range msg.Headers {
			if h.Key == "event_type" {
				eventType = string(h.Value)
				break
			}
		}

		dedupKey := orderID + ":" + eventType
		c.processedMu.Lock()
		if _, seen := c.processedKeys[dedupKey]; seen {
			c.processedMu.Unlock()
			c.logger.Warn("duplicate message received from kafka; skipping re-dispatch",
				"order_id", orderID, "event_type", eventType)
			_ = c.reader.CommitMessages(ctx, msg)
			continue
		}
		c.processedKeys[dedupKey] = struct{}{}
		c.processedMu.Unlock()

		switch eventType {
		case "OrderFulfilled":
			c.logger.Info("3PL dispatch: package queued for carrier pickup",
				"order_id", orderID, "partition", msg.Partition, "offset", msg.Offset)
		case "OrderCancelled":
			c.logger.Info("3PL dispatch: order cancelled; halting warehouse packing conveyor",
				"order_id", orderID)
		case "OrderExpired":
			c.logger.Info("3PL dispatch: abandoned checkout expired; no action required",
				"order_id", orderID)
		default:
			c.logger.Info("unhandled order event received",
				"order_id", orderID, "event_type", eventType)
		}

		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			c.logger.Error("failed to commit kafka message offset", "error", err)
		}
	}
}