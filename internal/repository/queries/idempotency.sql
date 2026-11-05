
-- name: GetIdempotencyKey :one
SELECT * FROM idempotency_key WHERE key = $1;


-- name: CreateIdempotencyKey :one
INSERT INTO idempotency_key(
    key,
    request_hash,
    state,
    created_at
) VALUES (
    $1 ,$2 , 'IN_PROGRESS' , NOW()
) RETURNING *;


-- name: CompleteIdempotencyKey :one
UPDATE idempotency_key
SET state = 'COMPLETED',
    response_status = $2,
    response_body = $3,
    completed_at = NOW()
WHERE key = $1
RETURNING *;
