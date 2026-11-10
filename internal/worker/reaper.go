package worker

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/yashyadav-4/ware_go/internal/repository/db"
)

type ReaperConfig struct {
	Interval  time.Duration
	BatchSize int32
}

type Reaper struct {
	pool    *pgxpool.Pool
	queries *db.Queries
	cfg     ReaperConfig
	logger  *slog.Logger
}

func NewReaper(pool *pgxpool.Pool , queries *db.Queries , cfg ReaperConfig ) *Reaper{
	if cfg.Interval <= 0{
		cfg.Interval = 1 * time.Second
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 100
	}

	return &Reaper{
		pool: pool,
		queries: queries,
		cfg: cfg,
		logger: slog.Default().With("component" , "reaper"),
	}
}

func (r *Reaper) Start(ctx context.Context){
	r.logger.Info("reaper daemon startedd", "interval", r.cfg.Interval, "batch_size", r.cfg.BatchSize)

	ticker := time.NewTicker(r.cfg.Interval)
	defer ticker.Stop();

	for {
		select{
		case <-ctx.Done():
			r.logger.Info("reaper daemon stopping...")
			return
		case <-ticker.C:
			const maxBatchesPerTick = 50
			for batch:= 0 ; batch < maxBatchesPerTick ; batch++{
				if ctx.Err() != nil{
					return
				}
				reapedCount , err := r.ReapExpiredOrders(ctx)
				if err != nil{
					r.logger.Error("reaper cycle failed" , "error", err)
					break
				}
				if reapedCount > 0 {
					r.logger.Info("reaped expired orders" , "count" , reapedCount)
				}

				if int32(reapedCount) < r.cfg.BatchSize{
					break;
				}
			}
		}
	}
}


func (r *Reaper) ReapExpiredOrders(ctx context.Context)(int , error){
	tx, err := r.pool.Begin(ctx)
	if err != nil{
		return 0, fmt.Errorf("failed to begin reaper transsaction tx : %w"  ,err)
	}
	defer tx.Rollback(ctx)

	qtx := r.queries.WithTx(tx)

	expiredOrders, err := qtx.ListExpiredOrdersForReclaim(ctx, r.cfg.BatchSize)
	if err != nil {
		return 0, fmt.Errorf("failed to query expired orders: %w", err)
	}

	if len(expiredOrders) == 0 {
		return 0, nil
	}

	for _ , order := range expiredOrders{
		items , err := qtx.ListOrderItems(ctx , order.ID)
		if err != nil {
			return 0, fmt.Errorf("failed to list items for expired order %v: %w", order.ID, err)
		}

		sort.Slice(items , func(i, j int)bool{
			cmp := bytes.Compare(items[i].WarehouseID.Bytes[:] , items[j].WarehouseID.Bytes[:])
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
				return 0, fmt.Errorf("failed to restore stock for sku %s: %w", item.Sku, err)
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
				return 0, fmt.Errorf("failed to write reaper audit log: %w", err)
			}
		}

		_, err = qtx.UpdateOrderStatus(ctx, db.UpdateOrderStatusParams{
			ID:     order.ID,
			Status: db.OrderStatusEXPIRED,
		})
		if err != nil {
			return 0, fmt.Errorf("failed to update order status to EXPIRED: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil{
		return 0, fmt.Errorf("failed to commit reaper transaction: %w", err)
	}
	return len(expiredOrders), nil
}


