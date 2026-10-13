-- Track fills so the DB reflects partial execution.
ALTER TABLE orders
ADD COLUMN IF NOT EXISTS filled_quantity DECIMAL(20,8) NOT NULL DEFAULT 0;

ALTER TABLE orders
ADD COLUMN IF NOT EXISTS remaining_quantity DECIMAL(20,8) NOT NULL DEFAULT 0;

-- Aggregate traded volume per event.
ALTER TABLE events
ADD COLUMN IF NOT EXISTS volume DECIMAL(20,8) NOT NULL DEFAULT 0;

-- Reconstruct existing fills before exposing the new lifecycle columns.
WITH fills AS (
    SELECT order_id, SUM(quantity) AS quantity
    FROM (
        SELECT taker_order_id AS order_id, quantity FROM trades
        UNION ALL
        SELECT maker_order_id AS order_id, quantity FROM trades
    ) legs
    GROUP BY order_id
)
UPDATE orders o
SET filled_quantity = CASE WHEN o.status = 'FILLED' THEN o.quantity
                          ELSE GREATEST(o.filled_quantity, COALESCE(f.quantity, 0)) END
FROM fills f
WHERE o.id = f.order_id;

UPDATE orders
SET filled_quantity = quantity
WHERE status = 'FILLED';

UPDATE orders
SET remaining_quantity = GREATEST(quantity - filled_quantity, 0);

UPDATE events e
SET volume = GREATEST(e.volume, COALESCE((
    SELECT SUM(t.quantity * t.price) FROM trades t WHERE t.event_id = e.id
), 0));

-- The engine supplies trade IDs so settlement is idempotent: redelivering
-- a trade hits the primary key and is ignored.
ALTER TABLE trades
ALTER COLUMN id DROP DEFAULT;

-- Remove the old spelling constraint BEFORE normalizing existing rows.
ALTER TABLE orders
DROP CONSTRAINT IF EXISTS orders_status_check;

UPDATE orders SET status = 'CANCELED' WHERE status = 'CANCELLED';

ALTER TABLE orders
ADD CONSTRAINT orders_status_check
CHECK (status IN ('PENDING', 'PARTIAL', 'FILLED', 'CANCELED', 'REJECTED'));
