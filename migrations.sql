CREATE TABLE IF NOT EXISTS affiliates (
    id SERIAL PRIMARY KEY,
    telegram_id INTEGER UNIQUE NOT NULL,
    name VARCHAR(255) NOT NULL,
    phone VARCHAR(20) NOT NULL,
    store_name VARCHAR(255) NOT NULL,
    balance NUMERIC(12, 2) DEFAULT 0.00 NOT NULL,
    total_sales NUMERIC(12, 2) DEFAULT 0.00 NOT NULL,
    total_orders INTEGER DEFAULT 0 NOT NULL
);

CREATE TABLE IF NOT EXISTS orders (
    id SERIAL PRIMARY KEY,
    affiliate_id INTEGER NOT NULL,
    customer_name VARCHAR(255) NOT NULL,
    customer_phone VARCHAR(20) NOT NULL,
    city VARCHAR(100) NOT NULL,
    product VARCHAR(255) NOT NULL,
    price NUMERIC(12, 2) NOT NULL,
    commission NUMERIC(12, 2) NOT NULL,
    status VARCHAR(50) DEFAULT 'pending' NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    CONSTRAINT fk_affiliate_order FOREIGN KEY (affiliate_id) REFERENCES affiliates(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS withdrawals (
    id SERIAL PRIMARY KEY,
    affiliate_id INTEGER NOT NULL,
    amount NUMERIC(12, 2) NOT NULL,
    phone VARCHAR(20) NOT NULL,
    status VARCHAR(50) DEFAULT 'pending' NOT NULL,
    requested_at TIMESTAMP WITH TIME ZONE NOT NULL,
    CONSTRAINT fk_affiliate_withdrawal FOREIGN KEY (affiliate_id) REFERENCES affiliates(id) ON DELETE CASCADE
);

-- مؤشرات لتحسين الأداء
CREATE INDEX IF NOT EXISTS idx_affiliates_telegram_id ON affiliates (telegram_id);
CREATE INDEX IF NOT EXISTS idx_orders_affiliate_id ON orders (affiliate_id);
CREATE INDEX IF NOT EXISTS idx_orders_created_at ON orders (created_at);
CREATE INDEX IF NOT EXISTS idx_withdrawals_affiliate_id ON withdrawals (affiliate_id);
ALTER TABLE orders ADD COLUMN product_code VARCHAR;
ALTER TABLE orders ADD COLUMN country VARCHAR NOT NULL DEFAULT 'السعودية';