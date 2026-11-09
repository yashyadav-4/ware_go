

-- name: UpsertRestockInventory :one
INSERT INTO inventory_items(
    warehouse_id,
    sku,
    available_qty,
    reserved_qty,
    version,
    updated_at
) VALUES(
    $1 , $2, $3, 0, 1, NOW()
)
ON CONFLICT (warehouse_id, sku)
DO UPDATE SET
    available_qty = inventory_items.available_qty + EXCLUDED.available_qty,
    updated_at = NOW()
RETURNING *;


-- name: InsertAuditlog :one
INSERT INTO inventory_audit_log(
    warehouse_id,
    sku,
    order_id,
    event_type,
    delta_available,
    delta_reserved,
    created_at
) VALUES (
    $1, $2, $3, $4, $5, $6, NOW()
)
RETURNING *;


-- name: GetInventoryByWarehouseAndSKU :one
SELECT * FROM inventory_items WHERE warehouse_id = $1 AND sku = $2 LIMIT 1;


-- name: ListInventoryBySKU :many   
SELECT i.* , w.name as warehouse_name , w.code as warehouse_code
FROM inventory_items i
JOIN warehouses w ON i.warehouse_id = w.id
WHERE i.sku = $1 AND  w.is_active = TRUE
ORDER BY i.available_qty DESC;


-- name: GetInventoryForUpdate :one
SELECT * FROM inventory_items WHERE warehouse_id = $1 AND sku = $2 FOR UPDATE;


-- name: ReserveInventoryStock :one
UPDATE inventory_items
SET available_qty = available_qty - $3,
    reserved_qty = reserved_qty + $3,
    version = version + 1,
    updated_at = NOW()
WHERE warehouse_id = $1 AND sku = $2
RETURNING *;


-- name: ReserveInventoryStockOptimistic :one
UPDATE inventory_items
SET available_qty = available_qty - $3,
    reserved_qty = reserved_qty + $3,
    version = version + 1,
    updated_at = NOW()
WHERE warehouse_id = $1 AND sku = $2 AND version = $4
RETURNING *;


-- name: ReserveInventoryStockAtomic :one
UPDATE inventory_items
SET available_qty = available_qty - $3,
    reserved_qty = reserved_qty + $3,
    version = version + 1,
    updated_at = NOW()
WHERE warehouse_id = $1 AND sku = $2 AND available_qty >= $3
RETURNING *;


-- name: ConfirmInventoryStock :one
UPDATE inventory_items
SET reserved_qty = reserved_qty - $3,
    version = version + 1,
    updated_at = NOW()
WHERE warehouse_id = $1 AND sku = $2
RETURNING *;


-- name: RollbackInventoryStock :one
UPDATE inventory_items
SET available_qty = available_qty + $3,
    reserved_qty = reserved_qty - $3,
    version = version + 1,
    updated_at = NOW()
WHERE warehouse_id = $1 AND sku = $2
RETURNING *;
