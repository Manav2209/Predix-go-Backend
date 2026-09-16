-- name: InsertTrade :one
INSERT INTO trades (
    id,
    event_id,
    outcome,
    taker_order_id,
    maker_order_id,
    buyer_id,
    seller_id,
    taker_side,
    quantity,
    price
)
VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10
)
RETURNING *;

-- name: GetTradesByEvent :many
SELECT *
FROM trades
WHERE event_id = $1
ORDER BY created_at DESC
LIMIT $2 OFFSET $3;