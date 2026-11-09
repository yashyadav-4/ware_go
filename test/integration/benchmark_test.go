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

func TestStrategyComparison(t *testing.T) {
	ctx := context.Background()
	pool, queries := setupTestDB(t)
	defer pool.Close()

	strategies := []struct {
		name     string
		strategy strategy.ReservationStrategy
	}{
		{"Pessimistic", strategy.NewPessimisticStrategy()},
		{"Optimistic_Jitter", strategy.NewOptimisticStrategy()},
		{"Atomic_Conditional", strategy.NewAtomicStrategy()},
	}

	for _, s := range strategies {
		t.Run(s.name, func(t *testing.T) {
			whCode := fmt.Sprintf("WH-BENCH-%d", time.Now().UnixNano()%10000000)
			wh, err := queries.CreateWarehouse(ctx, db.CreateWarehouseParams{
				Code:      whCode,
				Name:      "Bench WH",
				Latitude:  12.9716,
				Longitude: 77.5946,
				IsActive:  true,
			})
			if err != nil {
				t.Fatalf("failed to create warehouse: %v", err)
			}

			sku := fmt.Sprintf("SKU-%s-%d", s.name, time.Now().UnixNano()%10000000)
			_, err = queries.CreateProduct(ctx, db.CreateProductParams{
				Sku:             sku,
				Name:            "Bench Item",
				UnitWeightGrams: 200,
			})
			if err != nil {
				t.Fatalf("failed to create product: %v", err)
			}

			const initialStock = 50
			const totalRequests = 200

			_, err = queries.UpsertRestockInventory(ctx, db.UpsertRestockInventoryParams{
				WarehouseID:  wh.ID,
				Sku:          sku,
				AvailableQty: initialStock,
			})
			if err != nil {
				t.Fatalf("failed to restock: %v", err)
			}

			routingSvc := service.NewRoutingService(queries)
			resSvc := service.NewReservationService(pool, queries, routingSvc, s.strategy)

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

					_, err := resSvc.ReserveOrder(ctx, service.ReserveOrderRequest{
						IdempotencyKey: fmt.Sprintf("bench-%s-%d", sku, reqID),
						CustomerID:     custUUID,
						CustomerLat:    12.9716,
						CustomerLon:    77.5946,
						Items: []service.ItemRequest{
							{SKU: sku, Quantity: 1},
						},
						TTL: 5 * time.Minute,
					})

					if err == nil {
						atomic.AddInt64(&successCount, 1)
					} else {
						atomic.AddInt64(&failCount, 1)
					}
				}(i)
			}

			startTime := time.Now()
			close(startGate)
			wg.Wait()
			duration := time.Since(startTime)

			t.Logf("[%s] %d requests finished in %v (Success: %d, Failed: %d)",
				s.name, totalRequests, duration, successCount, failCount)

			if successCount != initialStock {
				t.Fatalf("[%s] Zero oversell violated: expected %d successes, got %d",
					s.name, initialStock, successCount)
			}
			if failCount != (totalRequests - initialStock) {
				t.Fatalf("[%s] Expected %d rejections, got %d",
					s.name, totalRequests-initialStock, failCount)
			}

			inv, err := queries.GetInventoryByWarehouseAndSKU(ctx, db.GetInventoryByWarehouseAndSKUParams{
				WarehouseID: wh.ID,
				Sku:         sku,
			})
			if err != nil {
				t.Fatalf("[%s] Failed to query final stock: %v", s.name, err)
			}

			if inv.AvailableQty != 0 {
				t.Fatalf("[%s] Expected available_qty=0, got %d", s.name, inv.AvailableQty)
			}
			if inv.ReservedQty != initialStock {
				t.Fatalf("[%s] Expected reserved_qty=%d, got %d", s.name, initialStock, inv.ReservedQty)
			}
		})
	}
}

func BenchmarkStrategies(b *testing.B) {
	ctx := context.Background()
	pool, queries := setupTestDB(b)
	defer pool.Close()

	strategies := []struct {
		name     string
		strategy strategy.ReservationStrategy
	}{
		{"Pessimistic", strategy.NewPessimisticStrategy()},
		{"Optimistic_Jitter", strategy.NewOptimisticStrategy()},
		{"Atomic_Conditional", strategy.NewAtomicStrategy()},
	}

	for _, s := range strategies {
		b.Run(s.name, func(b *testing.B) {
			whCode := fmt.Sprintf("WH-B-%d", time.Now().UnixNano()%10000000)
			wh, _ := queries.CreateWarehouse(ctx, db.CreateWarehouseParams{
				Code:      whCode,
				Name:      "Benchmark WH",
				Latitude:  12.9716,
				Longitude: 77.5946,
				IsActive:  true,
			})

			sku := fmt.Sprintf("SKU-B-%s-%d", s.name, time.Now().UnixNano()%10000000)
			_, _ = queries.CreateProduct(ctx, db.CreateProductParams{
				Sku:             sku,
				Name:            "Bench Item",
				UnitWeightGrams: 100,
			})

			_, _ = queries.UpsertRestockInventory(ctx, db.UpsertRestockInventoryParams{
				WarehouseID:  wh.ID,
				Sku:          sku,
				AvailableQty: int32(b.N + 1000),
			})

			routingSvc := service.NewRoutingService(queries)
			resSvc := service.NewReservationService(pool, queries, routingSvc, s.strategy)

			var reqCounter int64

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					id := atomic.AddInt64(&reqCounter, 1)
					var custUUID pgtype.UUID
					_ = custUUID.Scan(uuid.New().String())

					_, _ = resSvc.ReserveOrder(ctx, service.ReserveOrderRequest{
						IdempotencyKey: fmt.Sprintf("bench-p-%s-%d", sku, id),
						CustomerID:     custUUID,
						CustomerLat:    12.9716,
						CustomerLon:    77.5946,
						Items: []service.ItemRequest{
							{SKU: sku, Quantity: 1},
						},
						TTL: 5 * time.Minute,
					})
				}
			})
		})
	}
}