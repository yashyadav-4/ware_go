package api

import (
	"github.com/go-chi/chi/v5"
	"github.com/yashyadav-4/ware_go/internal/repository/db"
)

func RegisterRoutes(r chi.Router , queries *db.Queries){
	warehouseHandler := NewWarehouseHandler(queries)
	productHandler := NewProductHandler(queries)

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
	})
}