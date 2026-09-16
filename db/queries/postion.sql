-- name: UpsertPosition :exec
INSERT INTO positions (
    user_id,
    event_id,
    outcome,
    shares,
    avg_price
)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (user_id, event_id, outcome)
DO UPDATE SET
    shares = positions.shares + EXCLUDED.shares,
    avg_price = CASE
        WHEN positions.shares + EXCLUDED.shares = 0
        THEN EXCLUDED.avg_price
        ELSE (
            (positions.avg_price * positions.shares) +
            (EXCLUDED.avg_price * EXCLUDED.shares)
        ) / NULLIF(positions.shares + EXCLUDED.shares, 0)
    END,
    updated_at = NOW();

-- name: DecreasePosition :execrows
UPDATE positions
SET
    shares = shares - $4,
    updated_at = NOW()
WHERE user_id = $1
  AND event_id = $2
  AND outcome = $3
  AND shares >= $4;

-- name: GetPosition :one
SELECT *
FROM positions
WHERE user_id = $1
  AND event_id = $2
  AND outcome = $3;