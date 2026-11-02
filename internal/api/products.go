package api

import (
	"errors"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/yashyadav-4/ware_go/internal/repository/db"
)

type ProductHandler struct {
	queries *db.Queries
}

func NewProductHandler(queries *db.Queries) *ProductHandler {
	return &ProductHandler{queries: queries}
}

type CreateProductRequest struct {
	SKU             string `json:"sku"`
	Name            string `json:"name"`
	UnitWeightGrams int32  `json:"unit_weight_grams"`
}

func (h *ProductHandler) CreateProduct(w http.ResponseWriter, r *http.Request) {
	var req CreateProductRequest
	if err := ReadJSON(w, r, &req); err != nil {
		WriteError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}

	req.SKU = strings.TrimSpace(req.SKU)
	req.Name = strings.TrimSpace(req.Name)

	if req.SKU == "" {
		WriteError(w, http.StatusBadRequest, "sku is required")
		return
	}
	if req.Name == "" {
		WriteError(w, http.StatusBadRequest, "product name is required")
		return
	}
	if req.UnitWeightGrams <= 0 {
		WriteError(w, http.StatusBadRequest, "unit_weight_grams must be greater than 0")
		return
	}

	product, err := h.queries.CreateProduct(r.Context(), db.CreateProductParams{
		Sku:             req.SKU,
		Name:            req.Name,
		UnitWeightGrams: req.UnitWeightGrams,
	})
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			WriteError(w, http.StatusConflict, "product with this sku already exists")
			return
		}
		WriteError(w, http.StatusInternalServerError, "failed to create product")
		return
	}

	WriteJSON(w, http.StatusCreated, product)
}

func (h *ProductHandler) GetProduct(w http.ResponseWriter, r *http.Request) {
	sku := chi.URLParam(r, "sku")
	if sku == "" {
		WriteError(w, http.StatusBadRequest, "sku parameter is required")
		return
	}

	product, err := h.queries.GetProductBySKU(r.Context(), sku)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			WriteError(w, http.StatusNotFound, "product not found")
			return
		}
		WriteError(w, http.StatusInternalServerError, "failed to fetch product")
		return
	}

	WriteJSON(w, http.StatusOK, product)
}

func (h *ProductHandler) ListProducts(w http.ResponseWriter, r *http.Request) {
	products, err := h.queries.ListProducts(r.Context())
	if err != nil {
		WriteError(w, http.StatusInternalServerError, "failed to list products")
		return
	}

	WriteJSON(w, http.StatusOK, products)
}