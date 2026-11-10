package integration

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
	"github.com/yashyadav-4/ware_go/internal/repository/db"
	"github.com/yashyadav-4/ware_go/internal/service"
	"github.com/yashyadav-4/ware_go/internal/strategy"
	"github.com/yashyadav-4/ware_go/internal/worker"
)

type mockKafkaWriter struct {
	mu       sync.Mutex
	messages []kafka.Message
	err      error
}

func(m *mockKafkaWriter) WriteMessages(ctx context.Context, msgs ...kafka.Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return m.err
	}
	m.messages = append(m.messages, msgs...)
	return nil
}

func (m *mockKafkaWriter) Close() error {
	return nil
}

func cleanStaleOutboxRows(ctx context.Context, pool *pgxpool.Pool) {
	_, _ = pool.Exec(ctx, "UPDATE outbox SET status = 'PUBLISHED' WHERE status IN ('PENDING', 'PROCESSING')")
}

func TestOutboxRelay_PublishPendingEvents(t *testing.T) {
	ctx := context.Background()
	pool, queries := setupTestDB(t)
	defer pool.Close()
	cleanStaleOutboxRows(ctx, pool)

	whCode := fmt.Sprintf("WH-OUTBOX-%d", time.Now().UnixNano()%10000000)
	wh, err := queries.CreateWarehouse(ctx, db.CreateWarehouseParams{
		Code:      whCode,
		Name:      "Outbox WH",
		Latitude:  12.9716,
		Longitude: 77.5946,
		IsActive:  true,
	})
	if err != nil {
		t.Fatalf("failed to create warehouse: %v", err)
	}

	sku := fmt.Sprintf("SKU-OUTBOX-%d", time.Now().UnixNano()%10000000)
	_, err = queries.CreateProduct(ctx, db.CreateProductParams{
		Sku:             sku,
		Name:            "Outbox Item",
		UnitWeightGrams: 200,
	})
	if err != nil {
		t.Fatalf("failed to create product: %v", err)
	}

	_, err = queries.UpsertRestockInventory(ctx, db.UpsertRestockInventoryParams{
		WarehouseID:  wh.ID,
		Sku:          sku,
		AvailableQty: 10,
	})
	if err != nil {
		t.Fatalf("failed to restock: %v", err)
	}

	routingSvc := service.NewRoutingService(queries)
	resSvc := service.NewReservationService(pool, queries, routingSvc, strategy.NewPessimisticStrategy())

	var custUUID pgtype.UUID
	_ = custUUID.Scan(uuid.New().String())

	res, err := resSvc.ReserveOrder(ctx, service.ReserveOrderRequest{
		IdempotencyKey: fmt.Sprintf("idem-outbox-%s", sku),
		CustomerID:     custUUID,
		CustomerLat:    12.9716,
		CustomerLon:    77.5946,
		Items: []service.ItemRequest{
			{SKU: sku, Quantity: 2},
		},
		TTL: 5 * time.Minute,
	})
	if err != nil {
		t.Fatalf("failed to reserve: %v", err)
	}

	_, err = resSvc.ConfirmOrder(ctx, res.Order.ID)
	if err != nil {
		t.Fatalf("failed to confirm order: %v", err)
	}

	mockWriter := &mockKafkaWriter{}
	relay := worker.NewOutboxRelay(pool, queries, mockWriter, worker.OutboxRelayConfig{
		BatchSize: 100,
		Topic:     "orders.events",
	})

	count, err := relay.ProcessPendingEvents(ctx)
	if err != nil {
		t.Fatalf("failed to process pending events: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 processed event, got %d", count)
	}

	mockWriter.mu.Lock()
	if len(mockWriter.messages) != 1 {
		t.Fatalf("expected 1 message in kafka, got %d", len(mockWriter.messages))
	}
	msg := mockWriter.messages[0]
	mockWriter.mu.Unlock()

	expectedKey := uuid.UUID(res.Order.ID.Bytes).String()
	if string(msg.Key) != expectedKey {
		t.Fatalf("expected partition key %s, got %s", expectedKey, string(msg.Key))
	}

	var foundEventType bool
	for _, h := range msg.Headers {
		if h.Key == "event_type" && string(h.Value) == "OrderFulfilled" {
			foundEventType = true
			break
		}
	}
	if !foundEventType {
		t.Fatalf("expected header event_type=OrderFulfilled")
	}

	var outboxCount int
	err = pool.QueryRow(ctx, "SELECT count(*) FROM outbox WHERE aggregate_id = $1 AND status = 'PUBLISHED'", expectedKey).Scan(&outboxCount)
	if err != nil {
		t.Fatalf("failed to query outbox: %v", err)
	}
	if outboxCount != 1 {
		t.Fatalf("expected outbox status PUBLISHED, got %d rows", outboxCount)
	}
}

func TestOutboxRelay_RetryOnFailure(t *testing.T) {
	ctx := context.Background()
	pool, queries := setupTestDB(t)
	defer pool.Close()
	cleanStaleOutboxRows(ctx, pool)

	orderID := uuid.New().String()
	_, err := queries.InsertOutboxEvent(ctx, db.InsertOutboxEventParams{
		AggregateType: "ORDER",
		AggregateID:   orderID,
		EventType:     "OrderFulfilled",
		Payload:       []byte(`{"test":"retry"}`),
	})
	if err != nil {
		t.Fatalf("failed to insert test outbox event: %v", err)
	}

	failingWriter := &mockKafkaWriter{
		err: errors.New("kafka network connection timeout"),
	}

	relay := worker.NewOutboxRelay(pool, queries, failingWriter, worker.OutboxRelayConfig{
		BatchSize: 100,
		Topic:     "orders.events",
	})

	count, err := relay.ProcessPendingEvents(ctx)
	if err != nil {
		t.Fatalf("process pending events failed: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 event examined, got %d", count)
	}

	var attempts int
	var status string
	err = pool.QueryRow(ctx, "SELECT attempts, status FROM outbox WHERE aggregate_id = $1", orderID).Scan(&attempts, &status)
	if err != nil {
		t.Fatalf("failed to query event: %v", err)
	}

	if attempts != 1 {
		t.Fatalf("expected attempts = 1, got %d", attempts)
	}
	if status != "PENDING" {
		t.Fatalf("expected status to remain PENDING for retry, got %s", status)
	}
}

func TestOutboxRelay_ReclaimStaleLeases(t *testing.T) {
	ctx := context.Background()
	pool, queries := setupTestDB(t)
	defer pool.Close()

	orderID := uuid.New().String()
	_, err := pool.Exec(ctx, `
		INSERT INTO outbox (aggregate_type, aggregate_id, event_type, payload, status, locked_at, locked_by)
		VALUES ('ORDER', $1, 'OrderFulfilled', '{}', 'PROCESSING', NOW() - INTERVAL '40 seconds', 'dead-worker-1')
	`, orderID)
	if err != nil {
		t.Fatalf("failed to seed stale lease: %v", err)
	}

	if err := queries.ReclaimStaleOutboxLeases(ctx); err != nil {
		t.Fatalf("failed to reclaim stale leases: %v", err)
	}

	var status string
	var lockedBy *string
	err = pool.QueryRow(ctx, "SELECT status, locked_by FROM outbox WHERE aggregate_id = $1", orderID).Scan(&status, &lockedBy)
	if err != nil {
		t.Fatalf("failed to query reclaimed row: %v", err)
	}

	if status != "PENDING" {
		t.Fatalf("expected status reset to PENDING, got %s", status)
	}
	if lockedBy != nil {
		t.Fatalf("expected locked_by cleared, got %v", lockedBy)
	}
}