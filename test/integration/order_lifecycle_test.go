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
	"github.com/yashyadav-4/ware_go/internal/strategy"
)

func TestOrderLifecycle_Confirm(t *testing.T) {
	ctx := context.Background()
	pool, queries := setupTestDB(t)
	defer pool.Close()

	whCode := fmt.Sprintf("WH-CONFIRM-%d", time.Now().UnixNano()%10000000)
	wh, err := queries.CreateWarehouse(ctx, db.CreateWarehouseParams{
		Code:      whCode,
		Name:      "Confirm WH",
		Latitude:  12.9716,
		Longitude: 77.5946,
		IsActive:  true,
	})
	if err != nil {
		t.Fatalf("failed to create warehouse: %v", err)
	}

	sku := fmt.Sprintf("SKU-CONFIRM-%d", time.Now().UnixNano()%10000000)
	_, err = queries.CreateProduct(ctx, db.CreateProductParams{
		Sku:             sku,
		Name:            "Confirm Item",
		UnitWeightGrams: 500,
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
		IdempotencyKey: fmt.Sprintf("idem-confirm-%s", sku),
		CustomerID:     custUUID,
		CustomerLat:    12.9716,
		CustomerLon:    77.5946,
		Items: []service.ItemRequest{
			{SKU: sku, Quantity: orderQty},
		},
		TTL: 5 * time.Minute,
	})
	if err != nil {
		t.Fatalf("failed to reserve order: %v", err)
	}

	invAfterReserve, err := queries.GetInventoryByWarehouseAndSKU(ctx, db.GetInventoryByWarehouseAndSKUParams{
		WarehouseID: wh.ID,
		Sku:         sku,
	})
	if err != nil {
		t.Fatalf("failed to get inventory: %v", err)
	}
	if invAfterReserve.AvailableQty != (initialStock - orderQty) {
		t.Fatalf("expected available_qty=%d, got %d", initialStock-orderQty, invAfterReserve.AvailableQty)
	}
	if invAfterReserve.ReservedQty != orderQty {
		t.Fatalf("expected reserved_qty=%d, got %d", orderQty, invAfterReserve.ReservedQty)
	}

	confirmedOrder, err := resSvc.ConfirmOrder(ctx, res.Order.ID)
	if err != nil {
		t.Fatalf("failed to confirm order: %v", err)
	}
	if confirmedOrder.Status != db.OrderStatusCONFIRMED {
		t.Fatalf("expected returned order status CONFIRMED, got %s", confirmedOrder.Status)
	}

	invAfterConfirm, err := queries.GetInventoryByWarehouseAndSKU(ctx, db.GetInventoryByWarehouseAndSKUParams{
		WarehouseID: wh.ID,
		Sku:         sku,
	})
	if err != nil {
		t.Fatalf("failed to get inventory after confirm: %v", err)
	}

	if invAfterConfirm.AvailableQty != (initialStock - orderQty) {
		t.Fatalf("expected available_qty=%d, got %d", initialStock-orderQty, invAfterConfirm.AvailableQty)
	}
	if invAfterConfirm.ReservedQty != 0 {
		t.Fatalf("expected reserved_qty=0 after confirm, got %d", invAfterConfirm.ReservedQty)
	}

	dbOrder, err := queries.GetOrder(ctx, res.Order.ID)
	if err != nil {
		t.Fatalf("failed to fetch order: %v", err)
	}
	if dbOrder.Status != db.OrderStatusCONFIRMED {
		t.Fatalf("expected order status CONFIRMED, got %s", dbOrder.Status)
	}
}

func TestOrderLifecycle_Cancel(t *testing.T) {
	ctx := context.Background()
	pool, queries := setupTestDB(t)
	defer pool.Close()

	whCode := fmt.Sprintf("WH-CANCEL-%d", time.Now().UnixNano()%10000000)
	wh, err := queries.CreateWarehouse(ctx, db.CreateWarehouseParams{
		Code:      whCode,
		Name:      "Cancel WH",
		Latitude:  12.9716,
		Longitude: 77.5946,
		IsActive:  true,
	})
	if err != nil {
		t.Fatalf("failed to create warehouse: %v", err)
	}

	sku := fmt.Sprintf("SKU-CANCEL-%d", time.Now().UnixNano()%10000000)
	_, err = queries.CreateProduct(ctx, db.CreateProductParams{
		Sku:             sku,
		Name:            "Cancel Item",
		UnitWeightGrams: 500,
	})
	if err != nil {
		t.Fatalf("failed to create product: %v", err)
	}

	const initialStock = 10
	const orderQty = 4

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
		IdempotencyKey: fmt.Sprintf("idem-cancel-%s", sku),
		CustomerID:     custUUID,
		CustomerLat:    12.9716,
		CustomerLon:    77.5946,
		Items: []service.ItemRequest{
			{SKU: sku, Quantity: orderQty},
		},
		TTL: 5 * time.Minute,
	})
	if err != nil {
		t.Fatalf("failed to reserve order: %v", err)
	}

	cancelledOrder, err := resSvc.CancelOrder(ctx, res.Order.ID)
	if err != nil {
		t.Fatalf("failed to cancel order: %v", err)
	}
	if cancelledOrder.Status != db.OrderStatusCANCELLED {
		t.Fatalf("expected returned order status CANCELLED, got %s", cancelledOrder.Status)
	}

	invAfterCancel, err := queries.GetInventoryByWarehouseAndSKU(ctx, db.GetInventoryByWarehouseAndSKUParams{
		WarehouseID: wh.ID,
		Sku:         sku,
	})
	if err != nil {
		t.Fatalf("failed to get inventory after cancel: %v", err)
	}

	if invAfterCancel.AvailableQty != initialStock {
		t.Fatalf("expected available_qty restored to %d, got %d", initialStock, invAfterCancel.AvailableQty)
	}
	if invAfterCancel.ReservedQty != 0 {
		t.Fatalf("expected reserved_qty=0 after cancel, got %d", invAfterCancel.ReservedQty)
	}

	dbOrder, err := queries.GetOrder(ctx, res.Order.ID)
	if err != nil {
		t.Fatalf("failed to fetch order: %v", err)
	}
	if dbOrder.Status != db.OrderStatusCANCELLED {
		t.Fatalf("expected order status CANCELLED, got %s", dbOrder.Status)
	}
}

func TestOrderLifecycle_StateTransitions(t *testing.T) {
	ctx := context.Background()
	pool, queries := setupTestDB(t)
	defer pool.Close()

	whCode := fmt.Sprintf("WH-STATE-%d", time.Now().UnixNano()%10000000)
	wh, _ := queries.CreateWarehouse(ctx, db.CreateWarehouseParams{
		Code:      whCode,
		Name:      "State WH",
		Latitude:  12.9716,
		Longitude: 77.5946,
		IsActive:  true,
	})

	sku := fmt.Sprintf("SKU-STATE-%d", time.Now().UnixNano()%10000000)
	_, _ = queries.CreateProduct(ctx, db.CreateProductParams{
		Sku:             sku,
		Name:            "State Item",
		UnitWeightGrams: 100,
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
		IdempotencyKey: fmt.Sprintf("idem-state-%s", sku),
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

	if _, err := resSvc.ConfirmOrder(ctx, res.Order.ID); err != nil {
		t.Fatalf("first confirm failed: %v", err)
	}

	if _, err := resSvc.ConfirmOrder(ctx, res.Order.ID); err == nil {
		t.Fatalf("expected error when confirming an already CONFIRMED order, got nil")
	}

	if _, err := resSvc.CancelOrder(ctx, res.Order.ID); err == nil {
		t.Fatalf("expected error when cancelling an already CONFIRMED order, got nil")
	}
}