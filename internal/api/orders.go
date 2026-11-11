package api

import (
	"errors"
	"net/http"
	"strings"
	"time"   
	
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/yashyadav-4/ware_go/internal/repository/db"
	"github.com/yashyadav-4/ware_go/internal/service"
	"github.com/yashyadav-4/ware_go/internal/strategy"
)

type OrderHandler struct {
	queries *db.Queries
	reservationSvc *service.ReservationService
}

func NewOrderHandler(queries *db.Queries , reservationSvc *service.ReservationService) *OrderHandler {
	return &OrderHandler{
		queries:        queries,
		reservationSvc: reservationSvc,
	}
}

type OrderItemPayload struct{
	SKU      string `json:"sku"`
	Quantity int32  `json:"quantity"`
}

type ReserveOrderPayload struct {
	CustomerID    string             `json:"customer_id"`
	DestLatitude  float64            `json:"dest_latitude"`
	DestLongitude float64            `json:"dest_longitude"`
	Items         []OrderItemPayload `json:"items"`
	TTLSeconds    int64              `json:"ttl_seconds"`
}

type OrderDetailResponse struct {
	Order db.Order       `json:"order"`
	Items []db.OrderItem `json:"items"`
}


func (h *OrderHandler) Reserve(w http.ResponseWriter , r *http.Request){
	var payload ReserveOrderPayload
	if err := ReadJSON(w , r, &payload); err != nil {
		WriteError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}

	if len(payload.Items) == 0 {
		WriteError(w, http.StatusBadRequest, "items array cannot be empty")
		return
	}

	items := make([] service.ItemRequest , len(payload.Items))
	for i , item := range payload.Items{
		sku := strings.TrimSpace(item.SKU)
		if sku == ""{
			WriteError(w, http.StatusBadRequest, "item sku is required")
			return
		}

		if item.Quantity <= 0 {
			WriteError(w, http.StatusBadRequest, "item quantity must be greater than 0")
			return
		}

		items[i] = service.ItemRequest{
			SKU:      sku,
			Quantity: item.Quantity,
		}
	}

	var custUUID pgtype.UUID
	if strings.TrimSpace(payload.CustomerID) != ""{
		if err := custUUID.Scan(strings.TrimSpace(payload.CustomerID)); err != nil || !custUUID.Valid{
			WriteError(w, http.StatusBadRequest, "invalid customer_id: must be a valid UUID")
			return
		}
	}else{
		_ = custUUID.Scan(uuid.New().String())
	}

	idempotencyKey := r.Header.Get("Idempotency-Key")

	var reqTTL time.Duration
	if payload.TTLSeconds > 0 {
		reqTTL = time.Duration(payload.TTLSeconds) * time.Second
	}

	result , err := h.reservationSvc.ReserveOrder(r.Context() , service.ReserveOrderRequest{
		IdempotencyKey: idempotencyKey,
		CustomerID:     custUUID,
		CustomerLat:    payload.DestLatitude,
		CustomerLon:    payload.DestLongitude,
		Items:          items,
		TTL:            reqTTL,
	})


	if err != nil {
		if errors.Is(err , service.ErrOutOfStock) || errors.Is(err , strategy.ErrInsufficientStock){
			WriteError(w, http.StatusConflict, err.Error())
			return	
		}
		WriteError(w, http.StatusInternalServerError, "failed to reserve order: "+err.Error())
		return
	}
	WriteJSON(w, http.StatusCreated, result)
}

func (h *OrderHandler) GetOrder(w http.ResponseWriter , r *http.Request){
	idStr := chi.URLParam(r , "id")
	var orderUUID pgtype.UUID
	if err := orderUUID.Scan(strings.TrimSpace(idStr)) ; err != nil || !orderUUID.Valid {
		WriteError(w, http.StatusBadRequest, "invalid order id: must be a valid UUID")
		return
	}

	order , err := h.queries.GetOrder(r.Context() , orderUUID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			WriteError(w, http.StatusNotFound, "order not found")
			return
		}
		WriteError(w, http.StatusInternalServerError, "failed to fetch order: "+err.Error())
		return
	}

	items , err := h.queries.ListOrderItems(r.Context() , orderUUID)
	if err != nil {
		WriteError(w, http.StatusInternalServerError, "failed to fetch order items: "+err.Error())
		return
	}
	WriteJSON(w, http.StatusOK, OrderDetailResponse{
		Order: order,
		Items: items,
	})
}

func (h *OrderHandler) ConfirmOrder(w http.ResponseWriter , r *http.Request){
	idStr := chi.URLParam(r , "id")
	var orderUUID pgtype.UUID

	if err := orderUUID.Scan(strings.TrimSpace(idStr)); err != nil || !orderUUID.Valid {
		WriteError(w , http.StatusBadRequest , "invalid order id: must be a valid UUID")
		return
	}

	order , err := h.reservationSvc.ConfirmOrder(r.Context() , orderUUID)
	if err != nil {
		WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	WriteJSON(w, http.StatusOK, order)
}

func (h *OrderHandler) CancelOrder(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	var orderUUID pgtype.UUID
	if err := orderUUID.Scan(strings.TrimSpace(idStr)); err != nil || !orderUUID.Valid {
		WriteError(w, http.StatusBadRequest, "invalid order id: must be a valid UUID")
		return
	}
	order, err := h.reservationSvc.CancelOrder(r.Context(), orderUUID)
	if err != nil {
		WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	WriteJSON(w, http.StatusOK, order)
}