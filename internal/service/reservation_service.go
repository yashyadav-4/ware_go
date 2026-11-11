package service
import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"encoding/json"
	"time"
	"strings"

	"github.com/yashyadav-4/ware_go/internal/metrics"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/yashyadav-4/ware_go/internal/repository/db"
	"github.com/yashyadav-4/ware_go/internal/strategy"
)

type ReserveOrderRequest struct {
	IdempotencyKey string
	CustomerID     pgtype.UUID
	CustomerLat    float64
	CustomerLon    float64
	Items          []ItemRequest
	TTL            time.Duration 
}

type ReserveOrderResponse struct {
	Order       db.Order         `json:"order"`
	OrderItems  []db.OrderItem   `json:"order_items"`
	Allocations []AllocationPlan `json:"allocations"`
	Strategy    string           `json:"strategy"`
}

type ReservationService struct {
	pool       *pgxpool.Pool
	queries    *db.Queries
	routingSvc *RoutingService
	strategy   strategy.ReservationStrategy
}

func NewReservationService( pool *pgxpool.Pool, queries *db.Queries, routingSvc *RoutingService, strat strategy.ReservationStrategy) *ReservationService{
	return &ReservationService{
		pool:       pool,
		queries:    queries,
		routingSvc: routingSvc,
		strategy:   strat,
	}
}

func (s *ReservationService) ReserveOrder(ctx context.Context , req ReserveOrderRequest) (res *ReserveOrderResponse ,err error){

	start := time.Now()
	defer func(){
		duration := time.Since(start).Seconds()
		status := "success"
		if err != nil{
			status = "error"
			if strings.Contains(err.Error(), "OUT_OF_STOCK") || strings.Contains(err.Error(), "insufficient") {
				status = "out_of_stock"
			}	
		}

		stratName := "adaptive"
		if res != nil && res.Strategy != "" {
			stratName = res.Strategy
		}

		metrics.ReservationDuration.WithLabelValues(stratName , status).Observe(duration)
		metrics.ReservationsTotal.WithLabelValues(stratName , status).Inc()
	}()
	
	allocations , err := s.routingSvc.PlanAllocation(ctx, req.CustomerLat , req.CustomerLon , req.Items)
	if err != nil {
		return nil, fmt.Errorf("routing failed: %w", err)
	}

	sort.Slice(allocations , func(i , j int) bool{
		cmp := bytes.Compare(allocations[i].WarehouseID.Bytes[:] , allocations[j].WarehouseID.Bytes[:])
		if cmp != 0{
			return cmp < 0
		}
		return allocations[i].SKU < allocations[j].SKU
	})

	tx ,err := s.pool.Begin(ctx)
	if err != nil{
		return nil , fmt.Errorf("failed to begin transaction: %w" , err)
	}
	defer tx.Rollback(ctx)

	qtx := s.queries.WithTx(tx)

	stratItems := make([]strategy.ReservationItem , len(allocations))
	for i , alloc := range allocations{
		stratItems[i] = strategy.ReservationItem{
			WarehouseID: alloc.WarehouseID,
			SKU: alloc.SKU,
			Quantity: alloc.Quantity,
		}
	}

	if err := s.strategy.Reserve(ctx , qtx , stratItems); err != nil{
		return nil , fmt.Errorf("reservation strategy failed: %w" , err)
	}

	ttl := req.TTL
	if ttl <= 0{
		ttl = 10* time.Minute
	}

	expiresAt := time.Now().Add(ttl)
	var expiresAtTz pgtype.Timestamptz 
	_ = expiresAtTz.Scan(expiresAt)

	order , err := qtx.CreateOrder(ctx , db.CreateOrderParams{
		IdempotencyKey: req.IdempotencyKey,
		CustomerID:     req.CustomerID,
		DestLatitude:   req.CustomerLat,
		DestLongitude:  req.CustomerLon,
		ExpiresAt:      expiresAtTz,
	})

	if err != nil {
		return nil, fmt.Errorf("failed to create order: %w", err)
	}

	createdItems := make([]db.OrderItem , 0, len(allocations))

	for _ , alloc := range allocations{
		orderItem , err := qtx.CreateOrderItem(ctx , db.CreateOrderItemParams{
			OrderID:     order.ID,
			WarehouseID: alloc.WarehouseID,
			Sku:         alloc.SKU,
			Quantity:    alloc.Quantity,	
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create order item: %w", err)
		}

		createdItems = append(createdItems, orderItem)

		_, err = qtx.InsertAuditlog(ctx, db.InsertAuditlogParams{
			WarehouseID:    alloc.WarehouseID,
			Sku:            alloc.SKU,
			OrderID:        order.ID,
			EventType:      "RESERVE",
			DeltaAvailable: -alloc.Quantity,
			DeltaReserved:  alloc.Quantity,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to write audit log: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil{
		return nil , fmt.Errorf("failed to commit reservation transaction: %w", err)
	}
	return &ReserveOrderResponse{
		Order:       order,
		OrderItems:  createdItems,
		Allocations: allocations,
		Strategy:    s.strategy.Name(),
	}, nil
}

func (s *ReservationService) ConfirmOrder(ctx context.Context , orderID pgtype.UUID)(*db.Order, error){
	tx ,err:= s.pool.Begin(ctx);
	if err != nil {
		return nil , fmt.Errorf("failed to begin transaction : %w" , err)
	}
	defer tx.Rollback(ctx)

	qtx := s.queries.WithTx(tx)

	order , err := qtx.GetOrder(ctx , orderID)
	if err != nil{
		return nil , fmt.Errorf("order not found : %w" , err)
	}

	if order.Status != db.OrderStatusRESERVED {
		return nil, fmt.Errorf("cannot confirm order in status %s (must be RESERVED)", order.Status)
	}

	items, err := qtx.ListOrderItems(ctx , orderID)
	if err != nil {
		return nil , fmt.Errorf("failed to list order items: %w" ,err)
	}

	sort.Slice(items , func(i , j int) bool {
		cmp := bytes.Compare(items[i].WarehouseID.Bytes[:] , items[j].WarehouseID.Bytes[:])
		if cmp != 0 {
			return cmp < 0
		}
		return items[i].Sku < items[j].Sku
	})

	for _ , item := range items{
		_ , err := qtx.ConfirmInventoryStock(ctx , db.ConfirmInventoryStockParams{
			WarehouseID: item.WarehouseID,
			Sku:         item.Sku,
			ReservedQty: item.Quantity,
		})

		if err != nil {
			return nil, fmt.Errorf("failed to confirm inventory stock (sku=%s): %w", item.Sku, err)
		}

		_ , err = qtx.InsertAuditlog(ctx , db.InsertAuditlogParams{
			WarehouseID:    item.WarehouseID,
			Sku:            item.Sku,
			OrderID:        order.ID,
			EventType:      "COMMIT",
			DeltaAvailable: 0,
			DeltaReserved:  -item.Quantity,
		})

		if err != nil {
			return nil, fmt.Errorf("failed to write audit log: %w", err)
		}
	}

	updatedOrder , err := qtx.UpdateOrderStatus(ctx , db.UpdateOrderStatusParams{
		ID: order.ID,
		Status: db.OrderStatusCONFIRMED,
	})

	if err != nil {
		return nil , fmt.Errorf("failed to update order status: %w" , err)
	}
	
	//for kafka outbox
	orderIDStr := uuid.UUID(order.ID.Bytes).String()
	payloadBytes , _ := json.Marshal(map[string] interface{}{
		"order_id":     orderIDStr,
		"customer_id":  uuid.UUID(order.CustomerID.Bytes).String(),
		"status":       "CONFIRMED",
		"items_count":  len(items),
		"confirmed_at": time.Now().UTC(),
	})

	_, err = qtx.InsertOutboxEvent(ctx, db.InsertOutboxEventParams{
		AggregateType: "ORDER",
		AggregateID:   orderIDStr,
		EventType:     "OrderFulfilled",
		Payload:       payloadBytes,
	})

	if err != nil {
		return nil, fmt.Errorf("failed to write outbox event: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil , fmt.Errorf("failed to commit order confirmation: %w" , err)
	}

	return &updatedOrder , nil
}

func (s *ReservationService)CancelOrder(ctx context.Context, orderID pgtype.UUID) (*db.Order, error){
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	qtx := s.queries.WithTx(tx)

	order, err := qtx.GetOrder(ctx, orderID)
	if err != nil {
		return nil, fmt.Errorf("order not found: %w", err)
	}

	if order.Status != db.OrderStatusRESERVED {
		return nil, fmt.Errorf("cannot cancel order in status %s (must be RESERVED)", order.Status)
	}

	items, err := qtx.ListOrderItems(ctx, orderID)
	if err != nil {
		return nil, fmt.Errorf("failed to list order items: %w", err)
	}
	
	sort.Slice(items, func(i, j int) bool{
		cmp := bytes.Compare(items[i].WarehouseID.Bytes[:], items[j].WarehouseID.Bytes[:])
		if cmp != 0 {
			return cmp < 0
		}
		return items[i].Sku < items[j].Sku
	})

	for _, item := range items {
		_, err := qtx.RollbackInventoryStock(ctx, db.RollbackInventoryStockParams{
			WarehouseID:  item.WarehouseID,
			Sku:          item.Sku,
			AvailableQty: item.Quantity,
		})

		if err != nil {
			return nil, fmt.Errorf("failed to rollback inventory stock (sku=%s): %w", item.Sku, err)
		}

		_, err = qtx.InsertAuditlog(ctx, db.InsertAuditlogParams{
			WarehouseID:    item.WarehouseID,
			Sku:            item.Sku,
			OrderID:        order.ID,
			EventType:      "ROLLBACK",
			DeltaAvailable: item.Quantity,
			DeltaReserved:  -item.Quantity,
		})

		if err != nil {
			return nil, fmt.Errorf("failed to write audit log: %w", err)
		}
	}

	updatedOrder, err := qtx.UpdateOrderStatus(ctx, db.UpdateOrderStatusParams{
		ID:     order.ID,
		Status: db.OrderStatusCANCELLED,
	})

	if err != nil {
		return nil, fmt.Errorf("failed to update order status: %w", err)
	}

		orderIDStr := uuid.UUID(order.ID.Bytes).String()
	payloadBytes, _ := json.Marshal(map[string]interface{}{
		"order_id":     orderIDStr,
		"customer_id":  uuid.UUID(order.CustomerID.Bytes).String(),
		"status":       "CANCELLED",
		"cancelled_at": time.Now().UTC(),
	})

	_, err = qtx.InsertOutboxEvent(ctx, db.InsertOutboxEventParams{
		AggregateType: "ORDER",
		AggregateID:   orderIDStr,
		EventType:     "OrderCancelled",
		Payload:       payloadBytes,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to write outbox event: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit order cancellation: %w", err)
	}
	
	return &updatedOrder, nil
}