
package strategy

import (
	"context"
	"errors"
	"fmt"
	"github.com/yashyadav-4/ware_go/internal/repository/db"
)

var ErrInsufficientStock = errors.New("insufficient available stock for reservation")

type PessimisticStrategy struct {}


func NewPessimisticStrategy() *PessimisticStrategy {
	return &PessimisticStrategy{}
}

func (s *PessimisticStrategy) Name() string{
	return "pessimistic"
}

func(s *PessimisticStrategy) Reserve(ctx context.Context , qtx *db.Queries , items []ReservationItem) error{
	for _, item := range items{
		inv , err := qtx.GetInventoryForUpdate(ctx , db.GetInventoryForUpdateParams{
			WarehouseID: item.WarehouseID,
			Sku: item.SKU,
		})

		if err != nil {
			return fmt.Errorf("failed to lock inventory row (sku=%s): %w", item.SKU, err)
		}

		if inv.AvailableQty < item.Quantity{
			return fmt.Errorf("%w (sku=%s): required %d, available %d", ErrInsufficientStock, item.SKU, item.Quantity, inv.AvailableQty)
		}

		_ , err = qtx.ReserveInventoryStock(ctx ,db.ReserveInventoryStockParams{
			WarehouseID: item.WarehouseID,
			Sku: item.SKU,
			AvailableQty: item.Quantity,
		})

		if err != nil {
			return fmt.Errorf("failed to reserve inventory stock (sku=%s): %w", item.SKU, err)
		}
	}
	return nil
}