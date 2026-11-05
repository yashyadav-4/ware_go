
-- name: CreateOrder :one
INSERT INTO orders (
    idempotency_key,
    customer_id,
    status,
    dest_latitude,
    dest_longitude,
    expires_at,
    created_at,
    updated_at
) VALUES (
    $1 , $2 , 'RESERVED' , $3 , $4 , $5 , NOW() , NOW()
) RETURNING *;



-- name: CreateOrderItem :one
INSERT INTO order_items(
    order_id,
    warehouse_id,
    sku,
    quantity
) VALUES (
    $1 , $2 , $3 , $4
) RETURNING *;


-- name: GetOrder :one
SELECT * FROM orders WHERE id = $1;

-- name: ListOrderItems :many
SELECT * FROM order_items WHERE order_id = $1;

-- name: UpdateOrderStatus :one
UPDATE orders SET status = $2 , updated_at = NOW() WHERE id = $1 RETURNING *;

-- name: ListAvailableWarehousesForSku :many
SELECT
    w.id AS warehouse_id,
    w.code AS warehouse_code,
    w.name AS warehouse_name,
    w.latitude AS warehouse_latitude,
    w.longitude AS warehouse_longitude,
    i.available_qty,
    i.version
FROM inventory_items i 
JOIN warehouses w ON i.warehouse_id = w.id 
WHERE i.sku = $1
    AND w.is_active = TRUE
    AND i.available_qty > 0
ORDER BY i.available_qty DESC;