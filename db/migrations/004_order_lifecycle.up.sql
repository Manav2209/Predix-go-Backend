-- Track fills so the DB reflects partial execution.
ALTER TABLE orders
ADD COLUMN filled_quantity DECIMAL(20,8) NOT NULL DEFAULT 0;

ALTER TABLE orders
ADD COLUMN remaining_quantity DECIMAL(20,8) NOT NULL DEFAULT 0;

-- Aggregate traded volume per event.
ALTER TABLE events
ADD COLUMN volume DECIMAL(20,8) NOT NULL DEFAULT 0;

-- The engine supplies trade IDs so settlement is idempotent: redelivering
-- a trade hits the primary key and is ignored.
ALTER TABLE trades
ALTER COLUMN id DROP DEFAULT;

-- Normalize the status spelling to match the engine (CANCELED, one L).
UPDATE orders SET status = 'CANCELED' WHERE status = 'CANCELLED';

ALTER TABLE orders
DROP CONSTRAINT orders_status_check;

ALTER TABLE orders
ADD CONSTRAINT orders_status_check
CHECK (status IN ('PENDING', 'PARTIAL', 'FILLED', 'CANCELED', 'REJECTED'));