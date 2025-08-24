-- ===================================================
-- Monitoring Queries for WMS Logical Replication
-- Run these queries regularly to monitor system health
-- ===================================================

-- 1. Replication Status and Lag Monitoring
-- Run on PRIMARY server
SELECT 
    'PRIMARY SERVER STATUS' as server_type,
    pg_current_wal_lsn() as current_wal_lsn,
    pg_walfile_name(pg_current_wal_lsn()) as current_wal_file,
    pg_size_pretty(pg_current_wal_insert_lsn() - pg_current_wal_lsn()) as wal_insert_lag;

-- Run on REPLICA server
SELECT 
    'REPLICA SERVER STATUS' as server_type,
    pg_last_wal_receive_lsn() as last_received_lsn,
    pg_last_wal_replay_lsn() as last_replayed_lsn,
    pg_is_wal_replay_paused() as replay_paused;

-- 2. Replication Lag in Bytes and Time
-- Run on PRIMARY server
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
    pg_wal_lsn_diff(flush_lsn, replay_lsn) as replay_lag_bytes,
    -- Estimate lag in time (approximate)
    CASE 
        WHEN pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn) > 0 
        THEN ROUND(pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn) / 1024.0 / 1024.0 / 16.0, 2)
        ELSE 0 
    END as estimated_lag_minutes
FROM pg_stat_replication 
WHERE application_name = 'wms_analytics_replica';

-- 3. Publication and Subscription Status
-- Run on PRIMARY server
SELECT 
    'PUBLICATION STATUS' as status_type,
    pubname as publication_name,
    puballtables as all_tables,
    pubinsert as insert_enabled,
    pubupdate as update_enabled,
    pubdelete as delete_enabled
FROM pg_publication 
WHERE pubname = 'wms_analytics_pub';

-- Run on REPLICA server
SELECT 
    'SUBSCRIPTION STATUS' as status_type,
    subname as subscription_name,
    pid,
    received_lsn,
    latest_end_lsn,
    latest_end_time,
    status
FROM pg_stat_subscription 
WHERE subname = 'wms_analytics_sub';

-- 4. Replication Slot Status
-- Run on PRIMARY server
SELECT 
    slot_name,
    plugin,
    slot_type,
    active,
    restart_lsn,
    confirmed_flush_lsn,
    pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) as lag_size,
    pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) as lag_bytes
FROM pg_replication_slots 
WHERE slot_name = 'wms_analytics_slot';

-- 5. WAL Generation Rate and Disk Usage
-- Run on PRIMARY server
SELECT 
    'WAL STATISTICS' as metric_type,
    pg_size_pretty(pg_current_wal_lsn()) as current_wal_size,
    pg_size_pretty(pg_walfile_name_offset(pg_current_wal_lsn())) as current_wal_file_size,
    (SELECT setting FROM pg_settings WHERE name = 'wal_keep_segments') as wal_keep_segments,
    (SELECT setting FROM pg_settings WHERE name = 'max_wal_size') as max_wal_size;

-- 6. Table Replication Status
-- Run on REPLICA server to check which tables are being replicated
SELECT 
    schemaname,
    tablename,
    attname,
    n_distinct,
    correlation,
    most_common_vals,
    most_common_freqs
FROM pg_stats 
WHERE schemaname IN ('wms', 'inventory', 'orders', 'shipping', 'receiving', 'warehouse', 'locations', 'products', 'customers', 'suppliers')
ORDER BY schemaname, tablename, attname;

-- 7. Performance Monitoring for Analytics Views
-- Run on REPLICA server
SELECT 
    'ANALYTICS VIEW PERFORMANCE' as metric_type,
    schemaname,
    tablename,
    n_tup_ins as inserts,
    n_tup_upd as updates,
    n_tup_del as deletes,
    n_live_tup as live_tuples,
    n_dead_tup as dead_tuples,
    last_vacuum,
    last_autovacuum,
    last_analyze,
    last_autoanalyze
FROM pg_stat_user_tables 
WHERE schemaname LIKE '%analytics%'
ORDER BY schemaname, tablename;

-- 8. Connection and Query Statistics
-- Run on both servers to compare
SELECT 
    'CONNECTION STATS' as metric_type,
    datname,
    numbackends as active_connections,
    xact_commit as transactions_committed,
    xact_rollback as transactions_rolled_back,
    blks_read as blocks_read,
    blks_hit as blocks_hit,
    tup_returned as tuples_returned,
    tup_fetched as tuples_fetched,
    tup_inserted as tuples_inserted,
    tup_updated as tuples_updated,
    tup_deleted as tuples_deleted,
    temp_files as temporary_files,
    temp_bytes as temporary_bytes,
    deadlocks,
    blk_read_time as block_read_time_ms,
    blk_write_time as block_write_time_ms
FROM pg_stat_database 
WHERE datname = 'wms_db';

-- 9. Index Usage Statistics
-- Run on REPLICA server to optimize analytics queries
SELECT 
    'INDEX USAGE STATS' as metric_type,
    schemaname,
    tablename,
    indexname,
    idx_scan as index_scans,
    idx_tup_read as tuples_read,
    idx_tup_fetch as tuples_fetched
FROM pg_stat_user_indexes 
WHERE schemaname IN ('wms', 'inventory', 'orders', 'shipping', 'receiving', 'warehouse', 'locations', 'products', 'customers', 'suppliers')
ORDER BY idx_scan DESC;

-- 10. Lock Monitoring
-- Run on both servers to check for blocking
SELECT 
    'LOCK MONITORING' as metric_type,
    l.pid,
    l.mode,
    l.granted,
    a.usename,
    a.application_name,
    a.client_addr,
    a.query_start,
    a.state,
    a.query
FROM pg_locks l
JOIN pg_stat_activity a ON l.pid = a.pid
WHERE NOT l.granted
ORDER BY l.pid;

-- 11. Replication Conflicts Detection
-- Run on REPLICA server
SELECT 
    'REPLICATION CONFLICTS' as metric_type,
    pid,
    usename,
    application_name,
    backend_start,
    state,
    query_start,
    query
FROM pg_stat_activity 
WHERE application_name = 'wms_analytics_sub'
AND state = 'active';

-- 12. Disk Space Usage for WAL and Data
-- Run on PRIMARY server
SELECT 
    'DISK USAGE' as metric_type,
    pg_size_pretty(pg_database_size('wms_db')) as database_size,
    pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')) as total_wal_size,
    (SELECT setting FROM pg_settings WHERE name = 'data_directory') as data_directory;

-- 13. Autovacuum and Maintenance Status
-- Run on both servers
SELECT 
    'AUTOVACUUM STATUS' as metric_type,
    schemaname,
    tablename,
    last_vacuum,
    last_autovacuum,
    vacuum_count,
    autovacuum_count,
    last_analyze,
    last_autoanalyze,
    analyze_count,
    autoanalyze_count
FROM pg_stat_user_tables 
WHERE schemaname IN ('wms', 'inventory', 'orders', 'shipping', 'receiving', 'warehouse', 'locations', 'products', 'customers', 'suppliers')
ORDER BY last_autovacuum DESC NULLS LAST;
