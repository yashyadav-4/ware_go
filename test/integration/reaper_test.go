package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/yashyadav-4/ware_go/internal/repository/db"
	"github.com/yashyadav-4/ware_go/internal/service"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/yashyadav-4/ware_go/internal/strategy"
	"github.com/yashyadav-4/ware_go/internal/worker"
)

func cleanStaleTestReservations(ctx context.Context , pool *pgxpool.Pool){
	_, _ = pool.Exec(ctx, "UPDATE orders SET status = 'EXPIRED' WHERE status = 'RESERVED' AND expires_at < NOW()")
}

func TestReaper_ReclaimExpiredOrder(t *testing.T) {
	ctx := context.Background()
	pool, queries := setupTestDB(t)
	defer pool.Close()

	cleanStaleTestReservations(ctx ,pool) //need to clean db first because of old tests

	whCode := fmt.Sprintf("WH-REAP-%d", time.Now().UnixNano()%10000000)
	wh, err := queries.CreateWarehouse(ctx, db.CreateWarehouseParams{
		Code:      whCode,
		Name:      "Reaper WH",
		Latitude:  12.9716,
		Longitude: 77.5946,
		IsActive:  true,
	})
	if err != nil {
		t.Fatalf("failed to create warehouse: %v", err)
	}

	sku := fmt.Sprintf("SKU-REAP-%d", time.Now().UnixNano()%10000000)
	_, err = queries.CreateProduct(ctx, db.CreateProductParams{
		Sku:             sku,
		Name:            "Reap Item",
		UnitWeightGrams: 300,
	})
	if err != nil {
		t.Fatalf("failed to create product: %v", err)
	}

	const initialStock = 10
	const orderQty = 3

	_, err = queries.UpsertRestockInventory(ctx, db.UpsertRestockInventoryParams{
		WarehouseID:  wh.ID,
		Sku:          sku,
		AvailableQty: initialStock,
	})
	if err != nil {
		t.Fatalf("failed to restock: %v", err)
	}

	routingSvc := service.NewRoutingService(queries)
	resSvc := service.NewReservationService(pool, queries, routingSvc, strategy.NewPessimisticStrategy())

	var custUUID pgtype.UUID
	_ = custUUID.Scan(uuid.New().String())

	res, err := resSvc.ReserveOrder(ctx, service.ReserveOrderRequest{
		IdempotencyKey: fmt.Sprintf("reap-order-%s", sku),
		CustomerID:     custUUID,
		CustomerLat:    12.9716,
		CustomerLon:    77.5946,
		Items: []service.ItemRequest{
			{SKU: sku, Quantity: orderQty},
		},
		TTL: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("failed to reserve: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	reaper := worker.NewReaper(pool, queries, worker.ReaperConfig{
		BatchSize: 100,
		Interval:  1 * time.Second,
	})

	count, err := reaper.ReapExpiredOrders(ctx)
	if err != nil {
		t.Fatalf("reap expired orders failed: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 reaped order, got %d", count)
	}

	dbOrder, err := queries.GetOrder(ctx, res.Order.ID)
	if err != nil {
		t.Fatalf("failed to fetch order: %v", err)
	}
	if dbOrder.Status != db.OrderStatusEXPIRED {
		t.Fatalf("expected order status EXPIRED, got %s", dbOrder.Status)
	}

	inv, err := queries.GetInventoryByWarehouseAndSKU(ctx, db.GetInventoryByWarehouseAndSKUParams{
		WarehouseID: wh.ID,
		Sku:         sku,
	})
	if err != nil {
		t.Fatalf("failed to fetch inventory: %v", err)
	}
	if inv.AvailableQty != initialStock {
		t.Fatalf("expected available_qty restored to %d, got %d", initialStock, inv.AvailableQty)
	}
	if inv.ReservedQty != 0 {
		t.Fatalf("expected reserved_qty = 0, got %d", inv.ReservedQty)
	}
}

func TestReaper_SkipActiveReservations(t *testing.T) {
	ctx := context.Background()
	pool, queries := setupTestDB(t)
	defer pool.Close()

	cleanStaleTestReservations(ctx, pool)

	whCode := fmt.Sprintf("WH-ACTIVE-%d", time.Now().UnixNano()%10000000)
	wh, _ := queries.CreateWarehouse(ctx, db.CreateWarehouseParams{
		Code:      whCode,
		Name:      "Active WH",
		Latitude:  12.9716,
		Longitude: 77.5946,
		IsActive:  true,
	})

	sku := fmt.Sprintf("SKU-ACTIVE-%d", time.Now().UnixNano()%10000000)
	_, _ = queries.CreateProduct(ctx, db.CreateProductParams{
		Sku:             sku,
		Name:            "Active Item",
		UnitWeightGrams: 300,
	})

	_, _ = queries.UpsertRestockInventory(ctx, db.UpsertRestockInventoryParams{
		WarehouseID:  wh.ID,
		Sku:          sku,
		AvailableQty: 10,
	})

	routingSvc := service.NewRoutingService(queries)
	resSvc := service.NewReservationService(pool, queries, routingSvc, strategy.NewPessimisticStrategy())

	var custUUID pgtype.UUID
	_ = custUUID.Scan(uuid.New().String())

	res, err := resSvc.ReserveOrder(ctx, service.ReserveOrderRequest{
		IdempotencyKey: fmt.Sprintf("active-order-%s", sku),
		CustomerID:     custUUID,
		CustomerLat:    12.9716,
		CustomerLon:    77.5946,
		Items: []service.ItemRequest{
			{SKU: sku, Quantity: 2},
		},
		TTL: 10 * time.Minute,
	})
	if err != nil {
		t.Fatalf("failed to reserve: %v", err)
	}

	reaper := worker.NewReaper(pool, queries, worker.ReaperConfig{
		BatchSize: 100,
		Interval:  1 * time.Second,
	})

	count, err := reaper.ReapExpiredOrders(ctx)
	if err != nil {
		t.Fatalf("reaper failed: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected 0 reaped orders, got %d", count)
	}

	dbOrder, err := queries.GetOrder(ctx, res.Order.ID)
	if err != nil {
		t.Fatalf("failed to fetch order: %v", err)
	}
	if dbOrder.Status != db.OrderStatusRESERVED {
		t.Fatalf("expected order status to remain RESERVED, got %s", dbOrder.Status)
	}
}

func TestReaper_SLA_Within5Seconds(t *testing.T) {
	ctx := context.Background()
	pool, queries := setupTestDB(t)
	defer pool.Close()

	cleanStaleTestReservations(ctx, pool)

	whCode := fmt.Sprintf("WH-SLA-%d", time.Now().UnixNano()%10000000)
	wh, _ := queries.CreateWarehouse(ctx, db.CreateWarehouseParams{
		Code:      whCode,
		Name:      "SLA WH",
		Latitude:  12.9716,
		Longitude: 77.5946,
		IsActive:  true,
	})

	sku := fmt.Sprintf("SKU-SLA-%d", time.Now().UnixNano()%10000000)
	_, _ = queries.CreateProduct(ctx, db.CreateProductParams{
		Sku:             sku,
		Name:            "SLA Item",
		UnitWeightGrams: 100,
	})

	_, _ = queries.UpsertRestockInventory(ctx, db.UpsertRestockInventoryParams{
		WarehouseID:  wh.ID,
		Sku:          sku,
		AvailableQty: 5,
	})

	reaperCtx, cancelReaper := context.WithCancel(context.Background())
	defer cancelReaper()

	reaper := worker.NewReaper(pool, queries, worker.ReaperConfig{
		BatchSize: 100,
		Interval:  200 * time.Millisecond,
	})

	go reaper.Start(reaperCtx)

	routingSvc := service.NewRoutingService(queries)
	resSvc := service.NewReservationService(pool, queries, routingSvc, strategy.NewPessimisticStrategy())

	var custUUID pgtype.UUID
	_ = custUUID.Scan(uuid.New().String())

	res, err := resSvc.ReserveOrder(ctx, service.ReserveOrderRequest{
		IdempotencyKey: fmt.Sprintf("sla-order-%s", sku),
		CustomerID:     custUUID,
		CustomerLat:    12.9716,
		CustomerLon:    77.5946,
		Items: []service.ItemRequest{
			{SKU: sku, Quantity: 2},
		},
		TTL: 200 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("failed to reserve: %v", err)
	}

	startWait := time.Now()
	timeout := time.After(3 * time.Second)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	var reapedCleanly bool
	for {
		select {
		case <-timeout:
			t.Fatalf("CLAIM 4 VIOLATION: order was not reclaimed within 3 seconds (breached SLA)")
		case <-ticker.C:
			dbOrder, err := queries.GetOrder(ctx, res.Order.ID)
			if err == nil && dbOrder.Status == db.OrderStatusEXPIRED {
				reapedCleanly = true
				break
			}
		}
		if reapedCleanly {
			break
		}
	}

	elapsed := time.Since(startWait)
	t.Logf("CLAIM 4 VERIFIED: zombie stock reclaimed by background daemon in %v (SLA < 5s)", elapsed)
}