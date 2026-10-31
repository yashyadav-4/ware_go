
-- name: CreateProduct :one
INSERT INTO products(
    sku,
    name,
    unit_weight_grams
) VALUES(
    $1, $2 ,$3
)
RETURNING *;

-- name: GetProductBySKU :one
SELECT * FROM products WHERE sku = $1 LIMIT 1;

-- name: ListProducts :many
SELECT * FROM products ORDER BY created_at DESC;