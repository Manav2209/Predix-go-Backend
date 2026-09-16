-- name: InsertOrder :exec
INSERT INTO orders (
    id,
    event_id,
    user_id,
    order_type,
    outcome,
    side,
    quantity,
    price,
    status,
    filled_quantity,
    remaining_quantity
)
VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
);

-- name: UpdateOrderStatus :exec
UPDATE orders
SET status = $2, updated_at = NOW()
WHERE id = $1;

-- name: UpdateOrderFill :exec
UPDATE orders
SET
    filled_quantity = filled_quantity + $2,
    remaining_quantity = GREATEST(quantity - (filled_quantity + $2), 0),
    status = $3,
    updated_at = NOW()
WHERE id = $1;

-- name: GetOrdersByUserAndEvent :many
SELECT id, event_id, user_id, order_type, outcome, side, quantity, price, status, filled_quantity, remaining_quantity, created_at, updated_at
FROM orders
WHERE user_id = $1 AND event_id = $2;