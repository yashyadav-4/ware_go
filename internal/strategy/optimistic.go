package strategy

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/yashyadav-4/ware_go/internal/repository/db"
)

var ErrVersionMismatch = errors.New("optimistic lock version mismatch: max retries exceeded under contention")

type OptimisticStrategy struct{
	maxRetries int
}

func NewOptimisticStrategy() *OptimisticStrategy{
	return &OptimisticStrategy{
		maxRetries: 3,
	}
}

func (s *OptimisticStrategy) Name() string {
	return "optimistic"
}

func (s *OptimisticStrategy) Reserve(ctx context.Context, qtx *db.Queries, items []ReservationItem) error{
	for _, item := range items{
		success := false

		for attempt := 0; attempt < s.maxRetries; attempt++ {
			inv, err := qtx.GetInventoryByWarehouseAndSKU(ctx, db.GetInventoryByWarehouseAndSKUParams{
				WarehouseID: item.WarehouseID,
				Sku:         item.SKU,
			})
			if err != nil{
				return fmt.Errorf("failed to fetch inventory (sku=%s): %w", item.SKU, err)
			}

			if inv.AvailableQty < item.Quantity{
				return fmt.Errorf("%w (sku=%s): required %d , available %d",ErrInsufficientStock, item.SKU, item.Quantity, inv.AvailableQty)
			}

			_, err = qtx.ReserveInventoryStockOptimistic(ctx, db.ReserveInventoryStockOptimisticParams{
				WarehouseID:  item.WarehouseID,
				Sku:          item.SKU,
				AvailableQty: item.Quantity,
				Version:      inv.Version,
			})

			if err == nil{
				success= true
				break
			}

			if errors.Is(err, pgx.ErrNoRows) {
				baseDelay := time.Duration(5*(1<<attempt)) * time.Millisecond
				jitter := time.Duration(rand.IntN(10)) * time.Millisecond
				time.Sleep(baseDelay + jitter)
				continue
			}
			return fmt.Errorf("failed to reserve stock (sku=%s): %w", item.SKU, err)
		}

		if !success{
			return fmt.Errorf("%w (sku=%s): failed after %d attempts", ErrVersionMismatch, item.SKU, s.maxRetries)
		}
	}
	return nil
}