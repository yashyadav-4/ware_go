package integration

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/yashyadav-4/ware_go/internal/repository/db"
	"github.com/yashyadav-4/ware_go/internal/service"
	"github.com/yashyadav-4/ware_go/internal/strategy"
)

func TestConcurrentReservationZeroOversell(t *testing.T) {
	ctx := context.Background()
	pool, queries := setupTestDB(t)
	defer pool.Close()

	whCode := fmt.Sprintf("WH-CONCUR-%d", time.Now().UnixNano() % 10000000)
	wh, err := queries.CreateWarehouse(ctx, db.CreateWarehouseParams{
		Code:      whCode,
		Name:      "Concurrency Test WH",
		Latitude:  12.9716,
		Longitude: 77.5946,
		IsActive:  true,
	})
	if err != nil {
		t.Fatalf("failed to create warehouse: %v", err)
	}

	sku := fmt.Sprintf("SKU-CONCUR-%d", time.Now().UnixNano())
	_, err = queries.CreateProduct(ctx, db.CreateProductParams{
		Sku:             sku,
		Name:            "Hot Item 100 Units",
		UnitWeightGrams: 500,
	})
	if err != nil {
		t.Fatalf("failed to create product: %v", err)
	}

	const initialStock = 100
	_, err = queries.UpsertRestockInventory(ctx, db.UpsertRestockInventoryParams{
		WarehouseID:  wh.ID,
		Sku:          sku,
		AvailableQty: initialStock,
	})
	if err != nil {
		t.Fatalf("failed to restock: %v", err)
	}

	routingSvc := service.NewRoutingService(queries)
	pessimisticStrat := strategy.NewPessimisticStrategy()
	reservationSvc := service.NewReservationService(pool, queries, routingSvc, pessimisticStrat)

	const totalRequests = 1000
	var (
		successCount int64
		failCount    int64
		wg           sync.WaitGroup
	)

	startGate := make(chan struct{})
	wg.Add(totalRequests)

	for i := 0; i < totalRequests; i++ {
		go func(reqID int) {
			defer wg.Done()

			<-startGate

			var custUUID pgtype.UUID
			_ = custUUID.Scan(uuid.New().String())

			_, err := reservationSvc.ReserveOrder(ctx, service.ReserveOrderRequest{
				IdempotencyKey: fmt.Sprintf("concur-key-%s-%d", sku, reqID),
				CustomerID:     custUUID,
				CustomerLat:    12.9716,
				CustomerLon:    77.5946,
				Items: []service.ItemRequest{
					{
						SKU:      sku,
						Quantity: 1,
					},
				},
				TTL: 10 * time.Minute,
			})

			if err == nil {
				atomic.AddInt64(&successCount, 1)
			} else {
				atomic.AddInt64(&failCount, 1)
			}
		}(i)
	}

	close(startGate)
	wg.Wait()

	t.Logf("Concurrency run finished: %d succeeded, %d rejected", successCount, failCount)

	if successCount != initialStock {
		t.Fatalf("ZERO OVERSELL VIOLATION: expected exactly %d successes, got %d", initialStock, successCount)
	}
	if failCount != (totalRequests - initialStock) {
		t.Fatalf("expected exactly %d rejections, got %d", totalRequests-initialStock, failCount)
	}

	item, err := queries.GetInventoryByWarehouseAndSKU(ctx, db.GetInventoryByWarehouseAndSKUParams{
		WarehouseID: wh.ID,
		Sku:         sku,
	})
	if err != nil {
		t.Fatalf("failed to query inventory after test: %v", err)
	}

	if item.AvailableQty != 0 {
		t.Fatalf("INVARIANT VIOLATION: expected available_qty = 0, got %d", item.AvailableQty)
	}
	if item.ReservedQty != initialStock {
		t.Fatalf("INVARIANT VIOLATION: expected reserved_qty = %d, got %d", initialStock, item.ReservedQty)
	}

	t.Logf("CLAIM 1 VERIFIED: 1,000 requests for 100 units -> exactly 100 reserved, 900 rejected, available_qty=0, reserved_qty=100")
}