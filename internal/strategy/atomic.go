package strategy

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/yashyadav-4/ware_go/internal/repository/db"
)

type AtomicStrategy struct{}

func NewAtomicStrategy() *AtomicStrategy {
	return &AtomicStrategy{}
}

func (s *AtomicStrategy) Name() string{
	return "atomic"
}

func (s *AtomicStrategy) Reserve(ctx context.Context, qtx *db.Queries, items []ReservationItem) error{
	for _, item := range items {
		_, err := qtx.ReserveInventoryStockAtomic(ctx, db.ReserveInventoryStockAtomicParams{
			WarehouseID:  item.WarehouseID,
			Sku:          item.SKU,
			AvailableQty: item.Quantity,
		})

		if err != nil{
			if errors.Is(err, pgx.ErrNoRows){
				return fmt.Errorf("%w (sku=%s): required %d, but inventory had less or was out of stock", ErrInsufficientStock, item.SKU, item.Quantity)
			}
			return fmt.Errorf("failed to atomically reserve stock (sku=%s): %w", item.SKU, err)
		}
	}
	return nil
}