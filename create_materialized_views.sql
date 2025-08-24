-- ===================================================
-- Create Materialized Views for WMS Analytics
-- Replica Server - Performance Optimization
-- ===================================================

-- 1. Daily Warehouse Performance Summary
-- Refreshed daily for quick dashboard access
CREATE MATERIALIZED VIEW warehouse_analytics.daily_warehouse_summary AS
SELECT 
    DATE_TRUNC('day', CURRENT_DATE) as summary_date,
    w.warehouse_id,
    w.warehouse_name,
    w.location_code,
    COUNT(DISTINCT l.location_id) as total_locations,
    COUNT(DISTINCT CASE WHEN l.location_type = 'storage' THEN l.location_id END) as storage_locations,
    COUNT(DISTINCT CASE WHEN l.location_type = 'picking' THEN l.location_id END) as picking_locations,
    COUNT(DISTINCT CASE WHEN l.location_type = 'receiving' THEN l.location_id END) as receiving_locations,
    COUNT(DISTINCT CASE WHEN l.location_type = 'shipping' THEN l.location_id END) as shipping_locations,
    ROUND(AVG(l.utilization_rate), 2) as avg_utilization_rate,
    SUM(l.available_capacity) as total_available_capacity,
    SUM(l.used_capacity) as total_used_capacity,
    ROUND((SUM(l.used_capacity)::numeric / NULLIF(SUM(l.available_capacity), 0) * 100), 2) as overall_utilization_percent,
    COUNT(DISTINCT o.order_id) as total_orders_today,
    COUNT(DISTINCT s.shipment_id) as total_shipments_today,
    COUNT(DISTINCT r.receipt_id) as total_receipts_today
FROM warehouse.warehouses w
LEFT JOIN warehouse.locations l ON w.warehouse_id = l.warehouse_id
LEFT JOIN orders.orders o ON w.warehouse_id = o.warehouse_id 
    AND DATE_TRUNC('day', o.order_date) = DATE_TRUNC('day', CURRENT_DATE)
LEFT JOIN shipping.shipments s ON w.warehouse_id = s.warehouse_id 
    AND DATE_TRUNC('day', s.ship_date) = DATE_TRUNC('day', CURRENT_DATE)
LEFT JOIN receiving.receipts r ON w.warehouse_id = r.warehouse_id 
    AND DATE_TRUNC('day', r.receive_date) = DATE_TRUNC('day', CURRENT_DATE)
GROUP BY w.warehouse_id, w.warehouse_name, w.location_code;

-- Create unique index for fast refresh
CREATE UNIQUE INDEX CONCURRENTLY idx_daily_warehouse_summary_unique 
ON warehouse_analytics.daily_warehouse_summary(summary_date, warehouse_id);

-- 2. Monthly Inventory Summary
-- Refreshed monthly for inventory planning
CREATE MATERIALIZED VIEW inventory_analytics.monthly_inventory_summary AS
SELECT 
    DATE_TRUNC('month', CURRENT_DATE) as summary_month,
    p.product_id,
    p.product_name,
    p.product_category,
    p.sku,
    p.unit_of_measure,
    COALESCE(SUM(i.quantity_on_hand), 0) as total_quantity_on_hand,
    COALESCE(SUM(i.quantity_allocated), 0) as total_quantity_allocated,
    COALESCE(SUM(i.quantity_on_hand - i.quantity_allocated), 0) as available_quantity,
    COUNT(DISTINCT i.location_id) as locations_count,
    ROUND(AVG(i.unit_cost), 2) as avg_unit_cost,
    SUM(i.quantity_on_hand * i.unit_cost) as total_inventory_value,
    MAX(i.last_updated) as last_inventory_update,
    COUNT(DISTINCT im.movement_id) as movements_this_month,
    SUM(CASE WHEN im.movement_type = 'in' THEN im.quantity_moved ELSE 0 END) as total_quantity_in,
    SUM(CASE WHEN im.movement_type = 'out' THEN im.quantity_moved ELSE 0 END) as total_quantity_out,
    CASE 
        WHEN COALESCE(SUM(i.quantity_on_hand), 0) = 0 THEN 'Out of Stock'
        WHEN COALESCE(SUM(i.quantity_on_hand), 0) <= 10 THEN 'Low Stock'
        WHEN COALESCE(SUM(i.quantity_on_hand), 0) <= 50 THEN 'Medium Stock'
        ELSE 'Well Stocked'
    END as stock_status
FROM products.products p
LEFT JOIN inventory.inventory i ON p.product_id = i.product_id
LEFT JOIN inventory.inventory_movements im ON p.product_id = im.product_id 
    AND DATE_TRUNC('month', im.movement_date) = DATE_TRUNC('month', CURRENT_DATE)
GROUP BY p.product_id, p.product_name, p.product_category, p.sku, p.unit_of_measure;

-- Create unique index for fast refresh
CREATE UNIQUE INDEX CONCURRENTLY idx_monthly_inventory_summary_unique 
ON inventory_analytics.monthly_inventory_summary(summary_month, product_id);

-- 3. Weekly Order Performance Summary
-- Refreshed weekly for operational reporting
CREATE MATERIALIZED VIEW order_analytics.weekly_order_summary AS
SELECT 
    DATE_TRUNC('week', CURRENT_DATE) as summary_week,
    o.warehouse_id,
    w.warehouse_name,
    COUNT(DISTINCT o.order_id) as total_orders,
    COUNT(DISTINCT oi.order_item_id) as total_order_items,
    SUM(oi.quantity_ordered) as total_quantity_ordered,
    SUM(oi.quantity_shipped) as total_quantity_shipped,
    SUM(oi.quantity_backordered) as total_quantity_backordered,
    ROUND(AVG(oi.unit_price), 2) as avg_unit_price,
    SUM(oi.quantity_ordered * oi.unit_price) as total_order_value,
    ROUND(AVG(EXTRACT(EPOCH FROM (o.ship_date - o.order_date))/3600), 2) as avg_hours_to_ship,
    COUNT(DISTINCT CASE WHEN o.status = 'completed' THEN o.order_id END) as completed_orders,
    COUNT(DISTINCT CASE WHEN o.status = 'pending' THEN o.order_id END) as pending_orders,
    COUNT(DISTINCT CASE WHEN o.status = 'cancelled' THEN o.order_id END) as cancelled_orders,
    ROUND(
        COUNT(DISTINCT CASE WHEN o.status = 'completed' THEN o.order_id END)::numeric / 
        NULLIF(COUNT(DISTINCT o.order_id), 0) * 100, 2
    ) as completion_rate_percent,
    COUNT(DISTINCT o.customer_id) as unique_customers,
    ROUND(AVG(oi.quantity_ordered), 2) as avg_items_per_order
FROM orders.orders o
LEFT JOIN orders.order_items oi ON o.order_id = oi.order_id
LEFT JOIN warehouse.warehouses w ON o.warehouse_id = w.warehouse_id
WHERE DATE_TRUNC('week', o.order_date) = DATE_TRUNC('week', CURRENT_DATE)
GROUP BY DATE_TRUNC('week', CURRENT_DATE), o.warehouse_id, w.warehouse_name;

-- Create unique index for fast refresh
CREATE UNIQUE INDEX CONCURRENTLY idx_weekly_order_summary_unique 
ON order_analytics.weekly_order_summary(summary_week, warehouse_id);

-- 4. Customer Lifetime Value Summary
-- Refreshed monthly for customer analytics
CREATE MATERIALIZED VIEW customer_analytics.customer_lifetime_value AS
SELECT 
    DATE_TRUNC('month', CURRENT_DATE) as summary_month,
    c.customer_id,
    c.customer_name,
    c.customer_type,
    c.region,
    c.country,
    COUNT(DISTINCT o.order_id) as total_orders,
    SUM(oi.quantity_ordered * oi.unit_price) as total_spent,
    ROUND(AVG(oi.quantity_ordered * oi.unit_price), 2) as avg_order_value,
    MAX(o.order_date) as last_order_date,
    MIN(o.order_date) as first_order_date,
    ROUND(AVG(EXTRACT(EPOCH FROM (o.ship_date - o.order_date))/3600), 2) as avg_fulfillment_time_hours,
    COUNT(DISTINCT CASE WHEN o.status = 'completed' THEN o.order_id END) as completed_orders,
    COUNT(DISTINCT CASE WHEN o.status = 'cancelled' THEN o.order_id END) as cancelled_orders,
    ROUND(
        COUNT(DISTINCT CASE WHEN o.status = 'completed' THEN o.order_id END)::numeric / 
        NULLIF(COUNT(DISTINCT o.order_id), 0) * 100, 2
    ) as order_completion_rate,
    CASE 
        WHEN COUNT(DISTINCT o.order_id) >= 50 THEN 'VIP Customer'
        WHEN COUNT(DISTINCT o.order_id) >= 20 THEN 'Regular Customer'
        WHEN COUNT(DISTINCT o.order_id) >= 5 THEN 'Occasional Customer'
        ELSE 'New Customer'
    END as customer_segment,
    EXTRACT(DAYS FROM (CURRENT_DATE - MIN(o.order_date))) as customer_lifetime_days,
    ROUND(SUM(oi.quantity_ordered * oi.unit_price) / NULLIF(EXTRACT(DAYS FROM (CURRENT_DATE - MIN(o.order_date))), 0), 2) as daily_spending_rate
FROM customers.customers c
LEFT JOIN orders.orders o ON c.customer_id = o.customer_id
LEFT JOIN orders.order_items oi ON o.order_id = oi.order_id
GROUP BY c.customer_id, c.customer_name, c.customer_type, c.region, c.country;

-- Create unique index for fast refresh
CREATE UNIQUE INDEX CONCURRENTLY idx_customer_lifetime_value_unique 
ON customer_analytics.customer_lifetime_value(summary_month, customer_id);

-- 5. Product Movement Summary (Last 30 Days)
-- Refreshed daily for inventory movement tracking
CREATE MATERIALIZED VIEW inventory_analytics.product_movement_30days AS
SELECT 
    CURRENT_DATE as summary_date,
    p.product_id,
    p.product_name,
    p.product_category,
    p.sku,
    COUNT(DISTINCT im.movement_id) as total_movements,
    SUM(CASE WHEN im.movement_type = 'in' THEN im.quantity_moved ELSE 0 END) as total_quantity_in,
    SUM(CASE WHEN im.movement_type = 'out' THEN im.quantity_moved ELSE 0 END) as total_quantity_out,
    SUM(CASE WHEN im.movement_type = 'transfer' THEN im.quantity_moved ELSE 0 END) as total_quantity_transferred,
    ROUND(AVG(im.unit_cost), 2) as avg_unit_cost,
    SUM(im.quantity_moved * im.unit_cost) as total_movement_value,
    COUNT(DISTINCT im.from_location_id) as unique_from_locations,
    COUNT(DISTINCT im.to_location_id) as unique_to_locations,
    COUNT(DISTINCT w.warehouse_id) as warehouses_involved,
    MAX(im.movement_date) as last_movement_date,
    MIN(im.movement_date) as first_movement_date
FROM products.products p
LEFT JOIN inventory.inventory_movements im ON p.product_id = im.product_id 
    AND im.movement_date >= CURRENT_DATE - INTERVAL '30 days'
LEFT JOIN warehouse.locations fl ON im.from_location_id = fl.location_id
LEFT JOIN warehouse.locations tl ON im.to_location_id = tl.location_id
LEFT JOIN warehouse.warehouses w ON COALESCE(fl.warehouse_id, tl.warehouse_id) = w.warehouse_id
GROUP BY p.product_id, p.product_name, p.product_category, p.sku;

-- Create unique index for fast refresh
CREATE UNIQUE INDEX CONCURRENTLY idx_product_movement_30days_unique 
ON inventory_analytics.product_movement_30days(summary_date, product_id);

-- 6. Shipping and Receiving Daily Summary
-- Refreshed daily for operational efficiency tracking
CREATE MATERIALIZED VIEW warehouse_analytics.daily_shipping_receiving AS
SELECT 
    CURRENT_DATE as summary_date,
    w.warehouse_id,
    w.warehouse_name,
    'Shipping' as activity_type,
    COUNT(DISTINCT s.shipment_id) as total_shipments,
    SUM(si.quantity_shipped) as total_quantity_shipped,
    ROUND(AVG(EXTRACT(EPOCH FROM (s.ship_date - s.created_date))/3600), 2) as avg_hours_to_ship,
    COUNT(DISTINCT CASE WHEN s.status = 'shipped' THEN s.shipment_id END) as completed_shipments,
    COUNT(DISTINCT CASE WHEN s.status = 'pending' THEN s.shipment_id END) as pending_shipments,
    ROUND(
        COUNT(DISTINCT CASE WHEN s.status = 'shipped' THEN s.shipment_id END)::numeric / 
        NULLIF(COUNT(DISTINCT s.shipment_id), 0) * 100, 2
    ) as shipping_completion_rate
FROM warehouse.warehouses w
LEFT JOIN shipping.shipments s ON w.warehouse_id = s.warehouse_id 
    AND DATE_TRUNC('day', s.ship_date) = DATE_TRUNC('day', CURRENT_DATE)
LEFT JOIN shipping.shipment_items si ON s.shipment_id = si.shipment_id
GROUP BY w.warehouse_id, w.warehouse_name

UNION ALL

SELECT 
    CURRENT_DATE as summary_date,
    w.warehouse_id,
    w.warehouse_name,
    'Receiving' as activity_type,
    COUNT(DISTINCT r.receipt_id) as total_receipts,
    SUM(ri.quantity_received) as total_quantity_received,
    ROUND(AVG(EXTRACT(EPOCH FROM (r.receive_date - r.created_date))/3600), 2) as avg_hours_to_receive,
    COUNT(DISTINCT CASE WHEN r.status = 'received' THEN r.receipt_id END) as completed_receipts,
    COUNT(DISTINCT CASE WHEN r.status = 'pending' THEN r.receipt_id END) as pending_receipts,
    ROUND(
        COUNT(DISTINCT CASE WHEN r.status = 'received' THEN r.receipt_id END)::numeric / 
        NULLIF(COUNT(DISTINCT r.receipt_id), 0) * 100, 2
    ) as receiving_completion_rate
FROM warehouse.warehouses w
LEFT JOIN receiving.receipts r ON w.warehouse_id = r.warehouse_id 
    AND DATE_TRUNC('day', r.receive_date) = DATE_TRUNC('day', CURRENT_DATE)
LEFT JOIN receiving.receipt_items ri ON r.receipt_id = ri.receipt_id
GROUP BY w.warehouse_id, w.warehouse_name;

-- Create unique index for fast refresh
CREATE UNIQUE INDEX CONCURRENTLY idx_daily_shipping_receiving_unique 
ON warehouse_analytics.daily_shipping_receiving(summary_date, warehouse_id, activity_type);

-- 7. Create refresh functions for automated maintenance
CREATE OR REPLACE FUNCTION refresh_analytics_views()
RETURNS void AS $$
BEGIN
    -- Refresh daily views
    REFRESH MATERIALIZED VIEW CONCURRENTLY warehouse_analytics.daily_warehouse_summary;
    REFRESH MATERIALIZED VIEW CONCURRENTLY inventory_analytics.product_movement_30days;
    REFRESH MATERIALIZED VIEW CONCURRENTLY warehouse_analytics.daily_shipping_receiving;
    
    RAISE NOTICE 'Daily analytics views refreshed successfully';
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION refresh_weekly_analytics_views()
RETURNS void AS $$
BEGIN
    -- Refresh weekly views
    REFRESH MATERIALIZED VIEW CONCURRENTLY order_analytics.weekly_order_summary;
    
    RAISE NOTICE 'Weekly analytics views refreshed successfully';
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION refresh_monthly_analytics_views()
RETURNS void AS $$
BEGIN
    -- Refresh monthly views
    REFRESH MATERIALIZED VIEW CONCURRENTLY inventory_analytics.monthly_inventory_summary;
    REFRESH MATERIALIZED VIEW CONCURRENTLY customer_analytics.customer_lifetime_value;
    
    RAISE NOTICE 'Monthly analytics views refreshed successfully';
END;
$$ LANGUAGE plpgsql;

-- 8. Grant execute permissions on refresh functions
GRANT EXECUTE ON FUNCTION refresh_analytics_views() TO wms_analytics_user;
GRANT EXECUTE ON FUNCTION refresh_weekly_analytics_views() TO wms_analytics_user;
GRANT EXECUTE ON FUNCTION refresh_monthly_analytics_views() TO wms_analytics_user;

-- 9. Create cron-like scheduling using pg_cron extension (if available)
-- Note: This requires pg_cron extension to be installed
-- SELECT cron.schedule('refresh-daily-views', '0 2 * * *', 'SELECT refresh_analytics_views();');
-- SELECT cron.schedule('refresh-weekly-views', '0 3 * * 1', 'SELECT refresh_weekly_analytics_views();');
-- SELECT cron.schedule('refresh-monthly-views', '0 4 1 * *', 'SELECT refresh_monthly_analytics_views();');
