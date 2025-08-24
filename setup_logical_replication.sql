-- ===================================================
-- Setup Logical Replication for WMS Analytics
-- Primary Server Configuration
-- ===================================================

-- 1. Create publication for WMS tables
-- This will replicate all tables in the specified schemas
CREATE PUBLICATION wms_analytics_pub FOR ALL TABLES IN SCHEMA 
    public, 
    wms, 
    inventory, 
    orders, 
    shipping, 
    receiving,
    warehouse,
    locations,
    products,
    customers,
    suppliers;

-- Alternative: Create publication for specific tables only
-- CREATE PUBLICATION wms_analytics_pub FOR TABLE 
--     wms.orders,
--     wms.order_items,
--     wms.inventory_movements,
--     wms.warehouse_locations,
--     wms.products,
--     wms.customers;

-- 2. Grant necessary permissions to replication user
-- Create replication user if not exists
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'wms_repl_user') THEN
        CREATE ROLE wms_repl_user WITH REPLICATION LOGIN PASSWORD 'secure_repl_password_123';
    END IF;
END
$$;

-- Grant connect permission to replication user
GRANT CONNECT ON DATABASE wms_db TO wms_repl_user;

-- Grant usage on schemas
GRANT USAGE ON SCHEMA public TO wms_repl_user;
GRANT USAGE ON SCHEMA wms TO wms_repl_user;
GRANT USAGE ON SCHEMA inventory TO wms_repl_user;
GRANT USAGE ON SCHEMA orders TO wms_repl_user;
GRANT USAGE ON SCHEMA shipping TO wms_repl_user;
GRANT USAGE ON SCHEMA receiving TO wms_repl_user;
GRANT USAGE ON SCHEMA warehouse TO wms_repl_user;
GRANT USAGE ON SCHEMA locations TO wms_repl_user;
GRANT USAGE ON SCHEMA products TO wms_repl_user;
GRANT USAGE ON SCHEMA customers TO wms_repl_user;
GRANT USAGE ON SCHEMA suppliers TO wms_repl_user;

-- Grant select permission on all tables in schemas
GRANT SELECT ON ALL TABLES IN SCHEMA public TO wms_repl_user;
GRANT SELECT ON ALL TABLES IN SCHEMA wms TO wms_repl_user;
GRANT SELECT ON ALL TABLES IN SCHEMA inventory TO wms_repl_user;
GRANT SELECT ON ALL TABLES IN SCHEMA orders TO wms_repl_user;
GRANT SELECT ON ALL TABLES IN SCHEMA shipping TO wms_repl_user;
GRANT SELECT ON ALL TABLES IN SCHEMA receiving TO wms_repl_user;
GRANT SELECT ON ALL TABLES IN SCHEMA warehouse TO wms_repl_user;
GRANT SELECT ON ALL TABLES IN SCHEMA locations TO wms_repl_user;
GRANT SELECT ON ALL TABLES IN SCHEMA products TO wms_repl_user;
GRANT SELECT ON ALL TABLES IN SCHEMA customers TO wms_repl_user;
GRANT SELECT ON ALL TABLES IN SCHEMA suppliers TO wms_repl_user;

-- Grant select permission on future tables
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO wms_repl_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA wms GRANT SELECT ON TABLES TO wms_repl_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA inventory GRANT SELECT ON TABLES TO wms_repl_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA orders GRANT SELECT ON TABLES TO wms_repl_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA shipping GRANT SELECT ON TABLES TO wms_repl_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA receiving GRANT SELECT ON TABLES TO wms_repl_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA warehouse GRANT SELECT ON TABLES TO wms_repl_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA locations GRANT SELECT ON TABLES TO wms_repl_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA products GRANT SELECT ON TABLES TO wms_repl_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA customers GRANT SELECT ON TABLES TO wms_repl_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA suppliers GRANT SELECT ON TABLES TO wms_repl_user;

-- 3. Create replication slot for the replica
-- This ensures WAL segments are retained until consumed by the replica
SELECT pg_create_logical_replication_slot('wms_analytics_slot', 'pgoutput');

-- 4. Verify publication and slot creation
SELECT 
    pubname as publication_name,
    puballtables as all_tables,
    pubinsert as insert,
    pubupdate as update,
    pubdelete as delete
FROM pg_publication 
WHERE pubname = 'wms_analytics_pub';

SELECT 
    slot_name,
    plugin,
    slot_type,
    active,
    restart_lsn,
    confirmed_flush_lsn
FROM pg_replication_slots 
WHERE slot_name = 'wms_analytics_slot';

-- 5. Show current WAL position (useful for initial replica setup)
SELECT pg_current_wal_lsn() as current_wal_lsn;

-- 6. Monitor replication lag (run this periodically)
SELECT 
    pid,
    usename,
    application_name,
    client_addr,
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
FROM pg_stat_replication 
WHERE application_name = 'wms_analytics_replica';
