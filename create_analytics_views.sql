-- ===================================================
-- Create Analytics Views for WMS System
-- Replica Server - Analytics Layer
-- ===================================================

-- 1. Warehouse Performance Analytics
CREATE OR REPLACE VIEW warehouse_analytics.warehouse_performance AS
SELECT 
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
    ROUND((SUM(l.used_capacity)::numeric / NULLIF(SUM(l.available_capacity), 0) * 100), 2) as overall_utilization_percent
FROM warehouse.warehouses w
LEFT JOIN warehouse.locations l ON w.warehouse_id = l.warehouse_id
GROUP BY w.warehouse_id, w.warehouse_name, w.location_code;

-- 2. Inventory Analytics Dashboard
CREATE OR REPLACE VIEW inventory_analytics.inventory_summary AS
SELECT 
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
    CASE 
        WHEN COALESCE(SUM(i.quantity_on_hand), 0) = 0 THEN 'Out of Stock'
        WHEN COALESCE(SUM(i.quantity_on_hand), 0) <= 10 THEN 'Low Stock'
        WHEN COALESCE(SUM(i.quantity_on_hand), 0) <= 50 THEN 'Medium Stock'
        ELSE 'Well Stocked'
    END as stock_status
FROM products.products p
LEFT JOIN inventory.inventory i ON p.product_id = i.product_id
GROUP BY p.product_id, p.product_name, p.product_category, p.sku, p.unit_of_measure;

-- 3. Order Analytics and Performance
CREATE OR REPLACE VIEW order_analytics.order_performance AS
SELECT 
    DATE_TRUNC('day', o.order_date) as order_date,
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
    ) as completion_rate_percent
FROM orders.orders o
LEFT JOIN orders.order_items oi ON o.order_id = oi.order_id
LEFT JOIN warehouse.warehouses w ON o.warehouse_id = w.warehouse_id
GROUP BY DATE_TRUNC('day', o.order_date), o.warehouse_id, w.warehouse_name
ORDER BY order_date DESC, warehouse_id;

-- 4. Customer Analytics
CREATE OR REPLACE VIEW customer_analytics.customer_insights AS
SELECT 
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
    END as customer_segment
FROM customers.customers c
LEFT JOIN orders.orders o ON c.customer_id = o.customer_id
LEFT JOIN orders.order_items oi ON o.order_id = oi.order_id
GROUP BY c.customer_id, c.customer_name, c.customer_type, c.region, c.country
ORDER BY total_spent DESC;

-- 5. Shipping and Receiving Performance
CREATE OR REPLACE VIEW warehouse_analytics.shipping_receiving_performance AS
SELECT 
    DATE_TRUNC('day', COALESCE(s.ship_date, r.receive_date)) as activity_date,
    w.warehouse_id,
    w.warehouse_name,
    'Shipping' as activity_type,
    COUNT(DISTINCT s.shipment_id) as total_shipments,
    SUM(si.quantity_shipped) as total_quantity_shipped,
    ROUND(AVG(EXTRACT(EPOCH FROM (s.ship_date - s.created_date))/3600), 2) as avg_hours_to_ship,
    COUNT(DISTINCT CASE WHEN s.status = 'shipped' THEN s.shipment_id END) as completed_shipments
FROM warehouse.warehouses w
LEFT JOIN shipping.shipments s ON w.warehouse_id = s.warehouse_id
LEFT JOIN shipping.shipment_items si ON s.shipment_id = si.shipment_id
WHERE s.ship_date IS NOT NULL
GROUP BY DATE_TRUNC('day', s.ship_date), w.warehouse_id, w.warehouse_name

UNION ALL

SELECT 
    DATE_TRUNC('day', r.receive_date) as activity_date,
    w.warehouse_id,
    w.warehouse_name,
    'Receiving' as activity_type,
    COUNT(DISTINCT r.receipt_id) as total_receipts,
    SUM(ri.quantity_received) as total_quantity_received,
    ROUND(AVG(EXTRACT(EPOCH FROM (r.receive_date - r.created_date))/3600), 2) as avg_hours_to_receive,
    COUNT(DISTINCT CASE WHEN r.status = 'received' THEN r.receipt_id END) as completed_receipts
FROM warehouse.warehouses w
LEFT JOIN receiving.receipts r ON w.warehouse_id = r.warehouse_id
LEFT JOIN receiving.receipt_items ri ON r.receipt_id = ri.receipt_id
WHERE r.receive_date IS NOT NULL
GROUP BY DATE_TRUNC('day', r.receive_date), w.warehouse_id, w.warehouse_name
ORDER BY activity_date DESC, warehouse_id, activity_type;

-- 6. Product Movement Analytics
CREATE OR REPLACE VIEW inventory_analytics.product_movement_summary AS
SELECT 
    p.product_id,
    p.product_name,
    p.product_category,
    DATE_TRUNC('day', im.movement_date) as movement_date,
    im.movement_type,
    im.from_location_id,
    im.to_location_id,
    SUM(im.quantity_moved) as total_quantity_moved,
    COUNT(DISTINCT im.movement_id) as movement_count,
    ROUND(AVG(im.unit_cost), 2) as avg_unit_cost,
    SUM(im.quantity_moved * im.unit_cost) as total_movement_value,
    w.warehouse_name
FROM products.products p
JOIN inventory.inventory_movements im ON p.product_id = im.product_id
LEFT JOIN warehouse.locations fl ON im.from_location_id = fl.location_id
LEFT JOIN warehouse.locations tl ON im.to_location_id = tl.location_id
LEFT JOIN warehouse.warehouses w ON COALESCE(fl.warehouse_id, tl.warehouse_id) = w.warehouse_id
GROUP BY 
    p.product_id, p.product_name, p.product_category, 
    DATE_TRUNC('day', im.movement_date), im.movement_type,
    im.from_location_id, im.to_location_id, w.warehouse_name
ORDER BY movement_date DESC, product_id;

-- 7. Create indexes for better analytics performance
-- Note: These should be created after data replication is complete

-- Index for warehouse performance view
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_warehouse_performance_warehouse_id 
ON warehouse.warehouses(warehouse_id);

-- Index for inventory summary view
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_inventory_summary_product_id 
ON inventory.inventory(product_id);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_inventory_summary_location_id 
ON inventory.inventory(location_id);

-- Index for order performance view
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_order_performance_order_date 
ON orders.orders(order_date);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_order_performance_warehouse_id 
ON orders.orders(warehouse_id);

-- Index for customer insights view
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_customer_insights_customer_id 
ON customers.customers(customer_id);

-- Index for shipping receiving performance
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_shipments_warehouse_date 
ON shipping.shipments(warehouse_id, ship_date);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_receipts_warehouse_date 
ON receiving.receipts(warehouse_id, receive_date);

-- Index for product movement
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_inventory_movements_product_date 
ON inventory.inventory_movements(product_id, movement_date);
