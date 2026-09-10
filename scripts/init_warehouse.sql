-- ShipmentOps Data Warehouse Schema
-- Star schema with dimension tables and fact table

-- ETL Metadata table for watermarks
CREATE TABLE IF NOT EXISTS etl_metadata (
    table_name VARCHAR(100) PRIMARY KEY,
    last_watermark VARCHAR(50),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Dimension: Warehouse
CREATE TABLE IF NOT EXISTS dim_warehouse (
    warehouse_key SERIAL PRIMARY KEY,
    warehouse_id VARCHAR(50) UNIQUE NOT NULL,
    warehouse_name VARCHAR(200),
    region VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Dimension: Partner
CREATE TABLE IF NOT EXISTS dim_partner (
    partner_key SERIAL PRIMARY KEY,
    partner_id VARCHAR(50) UNIQUE NOT NULL,
    partner_name VARCHAR(200),
    partner_type VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Dimension: Route
CREATE TABLE IF NOT EXISTS dim_route (
    route_key SERIAL PRIMARY KEY,
    route_id VARCHAR(50) UNIQUE NOT NULL,
    origin VARCHAR(200),
    destination VARCHAR(200),
    distance_km INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Dimension: Date
CREATE TABLE IF NOT EXISTS dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE NOT NULL,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    month INTEGER NOT NULL,
    day INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

-- Fact: Deliveries
CREATE TABLE IF NOT EXISTS fact_deliveries (
    delivery_key SERIAL PRIMARY KEY,
    order_id VARCHAR(50) UNIQUE NOT NULL,
    warehouse_key INTEGER REFERENCES dim_warehouse(warehouse_key),
    partner_key INTEGER REFERENCES dim_partner(partner_key),
    route_key INTEGER REFERENCES dim_route(route_key),
    order_date_key INTEGER REFERENCES dim_date(date_key),
    promised_date_key INTEGER REFERENCES dim_date(date_key),
    actual_date_key INTEGER REFERENCES dim_date(date_key),
    order_value_usd DECIMAL(12, 2),
    is_on_time BOOLEAN,
    days_late INTEGER,
    delivery_duration_hours DECIMAL(10, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- indexes for query performance
CREATE INDEX IF NOT EXISTS idx_fact_deliveries_warehouse ON fact_deliveries(warehouse_key);
CREATE INDEX IF NOT EXISTS idx_fact_deliveries_partner ON fact_deliveries(partner_key);
CREATE INDEX IF NOT EXISTS idx_fact_deliveries_route ON fact_deliveries(route_key);
CREATE INDEX IF NOT EXISTS idx_fact_deliveries_order_date ON fact_deliveries(order_date_key);
CREATE INDEX IF NOT EXISTS idx_fact_deliveries_on_time ON fact_deliveries(is_on_time);

-- insert initial watermark
INSERT INTO etl_metadata (table_name, last_watermark) 
VALUES ('fact_deliveries', '1900-01-01')
ON CONFLICT (table_name) DO NOTHING;