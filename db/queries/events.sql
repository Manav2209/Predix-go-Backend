-- name: GetActiveEvents :many
SELECT id, title, description, question, thumbnail, is_active, expires_at, created_at, updated_at
FROM events
WHERE is_active = true
ORDER BY expires_at ASC;

-- name: GetEventByID :one
SELECT id, title, description, question, thumbnail, is_active, expires_at, created_at, updated_at
FROM events
WHERE id = $1;

-- name: CreateEvent :one
INSERT INTO events (id, title, description, question, thumbnail, expires_at)
VALUES (gen_random_uuid(), $1, $2, $3, $4, $5)
RETURNING id, title, description, question, thumbnail, is_active, expires_at, created_at, updated_at;

-- name: DeleteEvent :exec
DELETE FROM events WHERE id = $1;

-- name: IncrementEventVolume :exec
UPDATE events
SET volume = volume + $2, updated_at = NOW()
WHERE id = $1;

-- name: GetOrderByID :one
SELECT id, event_id, user_id, order_type, outcome, side, quantity, price, status, filled_quantity, remaining_quantity, created_at, updated_at
FROM orders
WHERE id = $1;