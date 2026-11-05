
package strategy

import (
	"context"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/yashyadav-4/ware_go/internal/repository/db"
)


//reservationItem represents a single sku allocation to be locked and reserved in a specific warehouse
type ReservationItem struct {
	WarehouseID pgtype.UUID `json:"warehouse_id"`
	SKU         string      `json:"sku"`
	Quantity    int32       `json:"quantity"`
}

// ReservationStrategy defines the contract for different concurrency locking strategies
//(pessimistic, optimistic CAS, atomic conditional)
type ReservationStrategy interface{
	Name() string
	Reserve(ctx context.Context , qtx *db.Queries , items []ReservationItem) error
}