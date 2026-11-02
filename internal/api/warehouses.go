package api

import (
	"errors"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/yashyadav-4/ware_go/internal/repository/db"
)

type WarehouseHandler struct {
	queries *db.Queries
}

func NewWarehouseHandler(queries *db.Queries) *WarehouseHandler{
	return &WarehouseHandler{queries: queries}
}


type CreateWarehouseRequest struct {
	Code      string  `json:"code"`
	Name      string  `json:"name"`
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

func (h *WarehouseHandler) CreateWarehouse(w http.ResponseWriter , r *http.Request){
	var req CreateWarehouseRequest;
	if err := ReadJSON(w, r, &req) ; err != nil {
		WriteError(w ,http.StatusBadRequest , "invalid request body: "+err.Error())
		return
	}

	req.Code = strings.TrimSpace(req.Code)
	req.Name = strings.TrimSpace(req.Name)

	if req.Code == "" {
		WriteError(w, http.StatusBadRequest, "warehouse code is required")
		return
	}
	if req.Name == "" {
		WriteError(w, http.StatusBadRequest, "warehouse name is required")
		return
	}
	if req.Latitude < -90.0 || req.Latitude > 90.0 {
		WriteError(w, http.StatusBadRequest, "latitude must be between -90 and 90")
		return
	}
	if req.Longitude < -180.0 || req.Longitude > 180.0 {
		WriteError(w, http.StatusBadRequest, "longitude must be between -180 and 180")
		return
	}

	warehouse , err := h.queries.CreateWarehouse(r.Context() , db.CreateWarehouseParams{
		Code:      req.Code,
		Name:      req.Name,
		Latitude:  req.Latitude,
		Longitude: req.Longitude,
		IsActive:  true,
	})

	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err , &pgErr) && pgErr.Code == "23505"{
			WriteError(w, http.StatusConflict, "warehouse code already exists")
			return
		}
		WriteError(w, http.StatusInternalServerError, "failed to create warehouse")
		return
	}

	WriteJSON(w, http.StatusCreated, warehouse)
}


func (h *WarehouseHandler) ListActiveWarehouses(w http.ResponseWriter, r *http.Request) {
	warehouses, err := h.queries.ListActiveWarehouses(r.Context())
	if err != nil {
		WriteError(w, http.StatusInternalServerError, "failed to list warehouses")
		return
	}
	WriteJSON(w, http.StatusOK, warehouses)
}	