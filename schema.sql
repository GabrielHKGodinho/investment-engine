CREATE TABLE orders (
	id             UUID PRIMARY KEY,
	user_id        UUID NOT NULL,
	asset_symbol   TEXT NOT NULL,
	quantity       INTEGER NOT NULL,
	side           TEXT NOT NULL CHECK (side IN ('BUY', 'SELL')),
	execution_type TEXT NOT NULL CHECK (execution_type IN ('MARKET', 'LIMIT')),
	limit_price    DOUBLE PRECISION,
	status         TEXT NOT NULL CHECK (status IN ('PENDING', 'EXECUTED', 'CANCELLED', 'REJECTED')),
	created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);