package service
import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
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

func (s *ReservationService) ReserveOrder(ctx context.Context , req ReserveOrderRequest) (*ReserveOrderResponse , error){
	
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