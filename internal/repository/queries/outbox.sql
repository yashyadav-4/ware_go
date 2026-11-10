

-- name: InsertOutboxEvent :one
INSERT INTO outbox (
    aggregate_type,
    aggregate_id,
    event_type,
    payload,
    status
) VALUES (
    $1, $2, $3, $4, 'PENDING'
) RETURNING *;


-- name: FetchPendingOutboxEvents :many
SELECT * FROM outbox
WHERE status = 'PENDING'
ORDER BY created_at ASC
FOR UPDATE SKIP LOCKED
LIMIT $1;


-- name: MarkOutboxEventPublished :exec
UPDATE outbox
SET 
    status = 'PUBLISHED',
    updated_at = NOW()
WHERE id = $1;


-- name: IncrementOutboxAttempts :exec
UPDATE outbox
SET 
    attempts = attempts + 1,
    status = CASE WHEN attempts +1 >= 5 THEN 'FAILED'::outbox_status ELSE 'PENDING'::outbox_status
END,
    updated_at = NOW()
WHERE id = $1;


-- name: ReclaimStaleOutboxLeases :exec
UPDATE outbox
SET 
    status = 'PENDING',
    locked_at = NULL,
    locked_by = NULL,
    updated_at = NOW()
WHERE status = 'PROCESSING'
  AND locked_at < NOW() - INTERVAL '30 seconds';

