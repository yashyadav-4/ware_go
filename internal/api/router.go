package api

import (
	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/yashyadav-4/ware_go/internal/api/middleware"
	"github.com/yashyadav-4/ware_go/internal/repository/db"
	"github.com/yashyadav-4/ware_go/internal/service"
	"github.com/yashyadav-4/ware_go/internal/agent"
	"github.com/yashyadav-4/ware_go/internal/strategy"
)

func RegisterRoutes(r chi.Router, pool *pgxpool.Pool, queries *db.Queries, rdb *redis.Client , rebalancer *agent.Rebalancer) {
	warehouseHandler := NewWarehouseHandler(queries)
	productHandler := NewProductHandler(queries)
	inventoryHandler := NewInventoryHandler(pool , queries, rdb)

	//service layer
	routingSvc := service.NewRoutingService(queries)
	adaptiveStrat := strategy.NewAdaptiveStrategy(rdb)
	reservationSvc := service.NewReservationService(pool, queries , routingSvc , adaptiveStrat) //nil defaults to pessimistic strategy

	orderHandler := NewOrderHandler(queries , reservationSvc)
	agentHandler := NewAgentHandler(rebalancer)

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

		r.Route("/orders" , func (r chi.Router){
			r.With(middleware.Idempotency(queries)).Post("/reserve" , orderHandler.Reserve) //with idempotency middleware
			r.Get("/{id}" , orderHandler.GetOrder)
			r.Post("/{id}/confirm", orderHandler.ConfirmOrder)
			r.Post("/{id}/cancel", orderHandler.CancelOrder)
		})

		r.Route("/agent", func(r chi.Router) {
			r.Post("/proposals", agentHandler.GenerateProposals)
			r.Post("/execute-transfer", agentHandler.ExecuteTransfer)
		})
	})
}