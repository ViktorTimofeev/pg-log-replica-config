-- ===================================================
-- Setup Logical Replication Replica for WMS Analytics
-- Replica Server Configuration
-- ===================================================

-- 1. Create subscription to the primary server
-- Replace connection parameters with your actual values
CREATE SUBSCRIPTION wms_analytics_sub
CONNECTION 'host=primary_wms_host port=5432 dbname=wms_db user=wms_repl_user password=secure_repl_password_123'
PUBLICATION wms_analytics_pub
WITH (
    copy_data = true,                    -- Copy existing data initially
    enabled = true,                      -- Enable subscription immediately
    create_slot = false,                 -- Slot already created on primary
    slot_name = 'wms_analytics_slot',    -- Use existing slot
    synchronous_commit = off,            -- Async commits for better performance
    streaming = on                       -- Enable streaming replication
);

-- 2. Create analytics-specific schemas and tables
-- These will be used for denormalized data and materialized views

-- Analytics schema for processed data
CREATE SCHEMA IF NOT EXISTS analytics;

-- Warehouse analytics schema
CREATE SCHEMA IF NOT EXISTS warehouse_analytics;

-- Inventory analytics schema
CREATE SCHEMA IF NOT EXISTS inventory_analytics;

-- Order analytics schema
CREATE SCHEMA IF NOT EXISTS order_analytics;

-- Customer analytics schema
CREATE SCHEMA IF NOT EXISTS customer_analytics;

-- 3. Create analytics users and roles
DO $$
BEGIN
    -- Analytics user for BI tools
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'wms_analytics_user') THEN
        CREATE ROLE wms_analytics_user WITH LOGIN PASSWORD 'analytics_password_123';
    END IF;
    
    -- Read-only user for reports
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'wms_reports_user') THEN
        CREATE ROLE wms_reports_user WITH LOGIN PASSWORD 'reports_password_123';
    END IF;
END
$$;

-- 4. Grant permissions to analytics users
GRANT CONNECT ON DATABASE wms_db TO wms_analytics_user;
GRANT CONNECT ON DATABASE wms_db TO wms_reports_user;

-- Grant usage on all schemas
GRANT USAGE ON ALL SCHEMAS IN DATABASE wms_db TO wms_analytics_user;
GRANT USAGE ON ALL SCHEMAS IN DATABASE wms_db TO wms_reports_user;

-- Grant select on all tables
GRANT SELECT ON ALL TABLES IN DATABASE wms_db TO wms_analytics_user;
GRANT SELECT ON ALL TABLES IN DATABASE wms_db TO wms_reports_user;

-- Grant create on analytics schemas
GRANT CREATE ON SCHEMA analytics TO wms_analytics_user;
GRANT CREATE ON SCHEMA warehouse_analytics TO wms_analytics_user;
GRANT CREATE ON SCHEMA inventory_analytics TO wms_analytics_user;
GRANT CREATE ON SCHEMA order_analytics TO wms_analytics_user;
GRANT CREATE ON SCHEMA customer_analytics TO wms_analytics_user;

-- 5. Set default privileges for future objects
ALTER DEFAULT PRIVILEGES IN SCHEMA analytics GRANT SELECT ON TABLES TO wms_reports_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA warehouse_analytics GRANT SELECT ON TABLES TO wms_reports_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA inventory_analytics GRANT SELECT ON TABLES TO wms_reports_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA order_analytics GRANT SELECT ON TABLES TO wms_reports_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA customer_analytics GRANT SELECT ON TABLES TO wms_reports_user;

-- 6. Verify subscription status
SELECT 
    subname as subscription_name,
    pid,
    received_lsn,
    latest_end_lsn,
    latest_end_time,
    status
FROM pg_stat_subscription 
WHERE subname = 'wms_analytics_sub';

-- 7. Monitor replication lag
SELECT 
    pid,
    usename,
    application_name,
    backend_start,
    state,
    sent_lsn,
    write_lsn,
    flush_lsn,
    replay_lsn,
    pg_wal_lsn_diff(pg_current_wal_lsn(), sent_lsn) as sent_lag_bytes,
    pg_wal_lsn_diff(sent_lsn, write_lsn) as write_lag_bytes,
    pg_wal_lsn_diff(write_lsn, flush_lsn) as flush_lag_bytes,
    pg_wal_lsn_diff(flush_lsn, replay_lsn) as replay_lag_bytes
FROM pg_stat_replication;

-- 8. Check for replication conflicts
SELECT 
    pid,
    usename,
    application_name,
    backend_start,
    state,
    query
FROM pg_stat_activity 
WHERE application_name = 'wms_analytics_sub'
AND state = 'active';

-- 9. Show subscription tables and their status
SELECT 
    schemaname,
    tablename,
    attname,
    n_distinct,
    correlation
FROM pg_stats 
WHERE schemaname IN ('wms', 'inventory', 'orders', 'shipping', 'receiving')
ORDER BY schemaname, tablename, attname;
