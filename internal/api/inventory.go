
package api


import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/yashyadav-4/ware_go/internal/repository/db"
)

type InventoryHandler struct {
	pool *pgxpool.Pool
	queries *db.Queries
	rdb *redis.Client
}

func NewInventoryHandler(pool *pgxpool.Pool ,queries *db.Queries , rdb *redis.Client) *InventoryHandler {
	return &InventoryHandler{
		pool: pool,
		queries: queries,
		rdb: rdb,
	}
}

type RestockRequest struct {
	WarehouseID string `json:"warehouse_id"`
	SKU         string `json:"sku"`
	Quantity    int32  `json:"quantity"`
}

type StockSummaryResponse struct{
	SKU            string                   `json:"sku"`
	TotalAvailable int32                    `json:"total_available"`
	Breakdown      []db.ListInventoryBySKURow `json:"breakdown"`
}

func (h *InventoryHandler) Restock(w http.ResponseWriter , r *http.Request){
	var req RestockRequest
	if err := ReadJSON(w, r , &req); err != nil{
		WriteError(w , http.StatusBadRequest , "invalid request body: "+err.Error())
		return
	}

	var whUUID pgtype.UUID
	if err := whUUID.Scan(strings.TrimSpace(req.WarehouseID)); err != nil || !whUUID.Valid {
		WriteError(w, http.StatusBadRequest, "invalid warehouse_id: must be a valid UUID")
		return
	}

	req.SKU = strings.TrimSpace(req.SKU)
	if req.SKU == "" {
		WriteError(w, http.StatusBadRequest, "sku is required")
		return
	}

	if req.Quantity <= 0 {
		WriteError(w, http.StatusBadRequest, "quantity must be greater than 0")
		return
	}

	//begin database transaction
	tx , err := h.pool.Begin(r.Context())
	if err != nil {
		WriteError(w, http.StatusInternalServerError, "failed to start transaction")
		return
	}
	defer tx.Rollback(r.Context()) //rollback before leaving above function no matter what

	qtx := h.queries.WithTx(tx)

	item , err := qtx.UpsertRestockInventory(r.Context() , db.UpsertRestockInventoryParams{
		WarehouseID: whUUID,
		Sku: req.SKU,
		AvailableQty: req.Quantity,
	})

	if err != nil {
		WriteError(w, http.StatusInternalServerError, "failed to update inventory: "+err.Error())
		return
	}

	_, err = qtx.InsertAuditlog(r.Context(), db.InsertAuditlogParams{
		WarehouseID:    whUUID,
		Sku:            req.SKU,
		OrderID:        pgtype.UUID{Valid: false}, //no order associated with restock
		EventType:      "RESTOCK",
		DeltaAvailable: req.Quantity,
		DeltaReserved:  0,
	})

	if err != nil {
		WriteError(w, http.StatusInternalServerError, "failed to write audit log: "+err.Error())
		return
	}

	if err := tx.Commit(r.Context()); err != nil {
		WriteError(w, http.StatusInternalServerError, "failed to commit transaction")
		return
	}

	cacheKey := "inventory:" + req.SKU
	_ = h.rdb.Del(r.Context() , cacheKey).Err()

	WriteJSON(w , http.StatusOK , item)
}

func (h *InventoryHandler) GetStockBySKU(w http.ResponseWriter , r *http.Request){
	sku := strings.TrimSpace(chi.URLParam(r, "sku"))
	if sku == "" {
		WriteError(w, http.StatusBadRequest, "sku parameter is required")
		return
	}

	cacheKey := "inventory:" + sku

	//1.try redis
	cachedBytes , err := h.rdb.Get(r.Context() , cacheKey).Bytes()
	if err == nil {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Cache", "HIT")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(cachedBytes)
		return
	}

	//2.cache miss
	rows ,err := h.queries.ListInventoryBySKU(r.Context() , sku)
	if err != nil{
		WriteError(w, http.StatusInternalServerError, "failed to fetch inventory")
		return
	}

	var total int32
	for _ , row := range rows {
		total += row.AvailableQty
	}

	resp := StockSummaryResponse{
		SKU:            sku,
		TotalAvailable: total,
		Breakdown:      rows,
	}

	if data, err := json.Marshal(resp); err == nil {
		_ = h.rdb.Set(r.Context() , cacheKey , data , 5*time.Second).Err()
	}

	w.Header().Set("X-Cache", "MISS")
	WriteJSON(w , http.StatusOK , resp)
}