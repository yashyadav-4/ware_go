
-- name: CreateWarehouse :one
INSERT INTO warehouses(
    code,
    name,
    latitude,
    longitude,
    is_active
) VALUES (
    $1 , $2, $3 , $4, $5
)
RETURNING *;

-- name: GetWarehouseByID :one
SELECT * FROM warehouses WHERE id = $1 LIMIT 1;

-- name: GetWarehouseByCode :one
SELECT * FROM warehouses WHERE code = $1 LIMIT 1;

-- name: ListActiveWarehouses :many
SELECT * FROM warehouses WHERE is_active = TRUE ORDER BY name ASC;