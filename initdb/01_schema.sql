CREATE USER airflow WITH PASSWORD 'airflow';
CREATE DATABASE airflow OWNER airflow;

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS raw.raw_clicks (
    event_id UUID PRIMARY KEY,
    user_id VARCHAR(64) NOT NULL,
    session_id VARCHAR(64) NOT NULL,
    event_type VARCHAR(32) NOT NULL,
    product_id VARCHAR(64) NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL,
    device_type VARCHAR(32) NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.raw_users (
    user_id VARCHAR(64) PRIMARY KEY,
    email VARCHAR(255) NOT NULL,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    country VARCHAR(100) NOT NULL,
    signup_date DATE NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.raw_products (
    product_id VARCHAR(64) PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(100) NOT NULL,
    price NUMERIC(12, 2) NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.raw_orders (
    order_id UUID PRIMARY KEY,
    user_id VARCHAR(64) NOT NULL,
    product_id VARCHAR(64) NOT NULL,
    session_id VARCHAR(64) NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price NUMERIC(12, 2) NOT NULL,
    order_timestamp TIMESTAMPTZ NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS analytics.realtime_click_aggregates (
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    event_type VARCHAR(32) NOT NULL,
    click_count BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (window_start, window_end, event_type)
);
