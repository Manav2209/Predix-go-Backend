ALTER TABLE orders
DROP CONSTRAINT orders_status_check;

ALTER TABLE orders
ADD CONSTRAINT orders_status_check
CHECK (status IN ('PENDING', 'FILLED', 'CANCELLED', 'PARTIAL', 'REJECTED'));

UPDATE orders SET status = 'CANCELLED' WHERE status = 'CANCELED';

ALTER TABLE trades
ALTER COLUMN id SET DEFAULT gen_random_uuid();

ALTER TABLE events
DROP COLUMN volume;

ALTER TABLE orders
DROP COLUMN remaining_quantity;

ALTER TABLE orders
DROP COLUMN filled_quantity;