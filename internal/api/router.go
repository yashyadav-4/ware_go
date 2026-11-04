package api

import (
	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/yashyadav-4/ware_go/internal/repository/db"
)

func RegisterRoutes(r chi.Router, pool *pgxpool.Pool, queries *db.Queries, rdb *redis.Client) {
	warehouseHandler := NewWarehouseHandler(queries)
	productHandler := NewProductHandler(queries)
	inventoryHandler := NewInventoryHandler(pool , queries, rdb)

	r.Route("/api/v1" , func(r chi.Router){
		r.Route("/warehouses" , func(r chi.Router){
			r.Post("/" , warehouseHandler.CreateWarehouse)
			r.Get("/" , warehouseHandler.ListActiveWarehouses)
		})

		r.Route("/products" , func(r chi.Router){
			r.Post("/" , productHandler.CreateProduct)
			r.Get("/" , productHandler.ListProducts)
			r.Get("/{sku}" , productHandler.GetProduct)
		})

		r.Route("/inventory" , func(r chi.Router){
			r.Post("/restock" , inventoryHandler.Restock)
			r.Get("/{sku}" , inventoryHandler.GetStockBySKU)
		})
	})
}