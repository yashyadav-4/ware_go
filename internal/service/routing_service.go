package service

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/yashyadav-4/ware_go/internal/repository/db"
	"github.com/yashyadav-4/ware_go/pkg/geo"
)

var ErrOutOfStock = errors.New("insufficient inventory across warehouses")

type ItemRequest struct {
	SKU string `json:"sku"`
	Quantity int32 `json:"quantity"`
}

type AllocationPlan struct{
	WarehouseID pgtype.UUID `json:"warehouse_id"`
	WarehouseCode string `json:"warehouse_code"`
	WarehouseName string `json:"warehouse_name"`
	SKU string `json:"sku"`
	Quantity int32 `json:"quantity"`
	DistanceKm float64 `json:"distance_km"`
	Cost float64 `json:"cost"`
}

type scoredCandidate struct{
	warehouse db.ListAvailableWarehousesForSkuRow
	distance float64
	cost float64
}

type RoutingService struct{
	queries *db.Queries
}

func NewRoutingService(queries *db.Queries) *RoutingService{
	return &RoutingService{queries: queries}
}

func (s *RoutingService) PlanAllocation(
	ctx context.Context, customerLat , customerLon float64 , items []ItemRequest ,
) ([]AllocationPlan , error) {
	var totalAllocation []AllocationPlan

	for _, item := range items{

		candidates , err := s.queries.ListAvailableWarehousesForSku(ctx , item.SKU)
		if err != nil{
			return nil , fmt.Errorf("failed to fetch warehouses for sku %s: %w" , item.SKU , err)
		}
		
		var totalAvailable int32

		for _ , c := range candidates{
			totalAvailable += c.AvailableQty
		}

		if totalAvailable < item.Quantity {
			return nil, fmt.Errorf("%w: sku %s requires %d, only %d available", ErrOutOfStock, item.SKU, item.Quantity, totalAvailable)
		}

		scored := make([]scoredCandidate , len(candidates))

		maxDist := 0.0

		for i , c := range candidates{
			dist := geo.HaversineKm(customerLat , customerLon , c.WarehouseLatitude , c.WarehouseLongitude)

			scored[i] = scoredCandidate{
				warehouse: c,
				distance: dist,
			}

			if dist > maxDist {
				maxDist = dist
			}
		}

		if maxDist == 0{
			maxDist = 1.0
		}

		for i:= range scored{
			normDist := scored[i].distance/ maxDist
			stockRatio := float64(scored[i].warehouse.AvailableQty) / float64(totalAvailable)
			scored[i].cost = (0.7 * normDist) + (0.3 * (1.0 - stockRatio))
		}

		sort.Slice(scored , func(i, j int) bool{
			return scored[i].cost < scored[j].cost
		})

		remainingToFill := item.Quantity

		for _ , cand:= range scored{
			if remainingToFill <= 0 {
				break
			}

			take := cand.warehouse.AvailableQty
			if take > remainingToFill{
				take = remainingToFill
			}

			remainingToFill -= take

			totalAllocation = append(totalAllocation , AllocationPlan{
				WarehouseID:   cand.warehouse.WarehouseID,
				WarehouseCode: cand.warehouse.WarehouseCode,
				WarehouseName: cand.warehouse.WarehouseName,
				SKU:           item.SKU,
				Quantity:      take,
				DistanceKm:    cand.distance,
				Cost:          cand.cost,
			})
		}
	}
	return totalAllocation , nil
}


