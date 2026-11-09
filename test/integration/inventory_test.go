package integration

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/yashyadav-4/ware_go/internal/config"
	"github.com/yashyadav-4/ware_go/internal/repository/db"
)

func setupTestDB(t testing.TB) (*pgxpool.Pool, *db.Queries) {
	t.Helper()
	cfg := config.Load()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		t.Fatalf("failed to connect to test database: %v", err)
	}

	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("failed to ping test database: %v", err)
	}

	return pool, db.New(pool)
}

func TestInventoryConstraints(t *testing.T) {
	ctx := context.Background()
	pool, queries := setupTestDB(t)
	defer pool.Close()

	//1.seed a test warehouse
	wh, err := queries.CreateWarehouse(ctx, db.CreateWarehouseParams{
		Code:      "TEST-WH-1",
		Name:      "Test Warehouse",
		Latitude:  12.97,
		Longitude: 77.59,
		IsActive:  true,
	})
	if err != nil {
		//if already exists from previous run, fetch it
		wh, _ = queries.GetWarehouseByCode(ctx, "TEST-WH-1")
	}

	//2.seed a test product
	prod, err := queries.CreateProduct(ctx, db.CreateProductParams{
		Sku:             "TEST-SKU-1",
		Name:            "Test Product",
		UnitWeightGrams: 500,
	})
	if err != nil {
		prod, _ = queries.GetProductBySKU(ctx, "TEST-SKU-1")
	}

	//3.test Restock & Read
	t.Run("restock adds positive stock correctly", func(t *testing.T) {
		item, err := queries.UpsertRestockInventory(ctx, db.UpsertRestockInventoryParams{
			WarehouseID:  wh.ID,
			Sku:          prod.Sku,
			AvailableQty: 100,
		})
		if err != nil {
			t.Fatalf("restock failed: %v", err)
		}
		if item.AvailableQty < 100 {
			t.Errorf("expected at least 100 available, got %d", item.AvailableQty)
		}
	})

	//4.test Engine Rejection: attempt negative available_qty
	t.Run("CHECK constraint rejects negative available stock", func(t *testing.T) {
		// intentionally attempt to set available_qty to -10 directly via SQL qqueries
		_, err := pool.Exec(ctx,
			"UPDATE inventory_items SET available_qty = -10 WHERE warehouse_id = $1 AND sku = $2",
			wh.ID, prod.Sku,
		)

		if err == nil {
			t.Fatal("expected database to reject negative stock, but query succeeded!")
		}

		//verify that PostgreSQL returned SQLSTATE 23514 (check_violation)
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "23514" {
			t.Fatalf("expected SQLSTATE 23514 (check_violation), got error: %v", err)
		}

		t.Logf("Success: Database rejected negative stock with SQLSTATE %s (%s)", pgErr.Code, pgErr.ConstraintName)
	})
}

// go test -v ./test/integration/...