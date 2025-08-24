-- ===================================================
-- Примеры сложных аналитических запросов для WMS системы
-- Replica Server - Analytics Layer
-- ===================================================

-- 1. Анализ эффективности складов по месяцам
-- Сравнение производительности складов по ключевым метрикам
WITH warehouse_monthly_stats AS (
    SELECT 
        DATE_TRUNC('month', o.order_date) as month,
        w.warehouse_id,
        w.warehouse_name,
        w.region,
        COUNT(DISTINCT o.order_id) as total_orders,
        SUM(oi.quantity_ordered * oi.unit_price) as total_revenue,
        ROUND(AVG(EXTRACT(EPOCH FROM (o.ship_date - o.order_date))/3600), 2) as avg_fulfillment_hours,
        COUNT(DISTINCT o.customer_id) as unique_customers,
        SUM(oi.quantity_ordered) as total_quantity_ordered,
        SUM(oi.quantity_shipped) as total_quantity_shipped,
        ROUND(
            SUM(oi.quantity_shipped)::numeric / NULLIF(SUM(oi.quantity_ordered), 0) * 100, 2
        ) as fulfillment_rate_percent
    FROM warehouse.warehouses w
    LEFT JOIN orders.orders o ON w.warehouse_id = o.warehouse_id
    LEFT JOIN orders.order_items oi ON o.order_id = oi.order_id
    WHERE o.order_date >= CURRENT_DATE - INTERVAL '12 months'
    GROUP BY DATE_TRUNC('month', o.order_date), w.warehouse_id, w.warehouse_name, w.region
),
warehouse_rankings AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY month ORDER BY total_revenue DESC) as revenue_rank,
        ROW_NUMBER() OVER (PARTITION BY month ORDER BY avg_fulfillment_hours ASC) as speed_rank,
        ROW_NUMBER() OVER (PARTITION BY month ORDER BY fulfillment_rate_percent DESC) as fulfillment_rank
    FROM warehouse_monthly_stats
)
SELECT 
    month,
    warehouse_name,
    region,
    total_orders,
    total_revenue,
    avg_fulfillment_hours,
    unique_customers,
    fulfillment_rate_percent,
    revenue_rank,
    speed_rank,
    fulfillment_rank,
    CASE 
        WHEN revenue_rank <= 3 AND speed_rank <= 3 AND fulfillment_rank <= 3 THEN 'Top Performer'
        WHEN revenue_rank <= 5 AND speed_rank <= 5 AND fulfillment_rank <= 5 THEN 'High Performer'
        WHEN revenue_rank <= 8 AND speed_rank <= 8 AND fulfillment_rank <= 8 THEN 'Average Performer'
        ELSE 'Needs Improvement'
    END as performance_category
FROM warehouse_rankings
ORDER BY month DESC, revenue_rank;

-- 2. Анализ трендов инвентаря и прогнозирование
-- Анализ движения товаров и прогноз потребности в пополнении
WITH inventory_trends AS (
    SELECT 
        p.product_id,
        p.product_name,
        p.product_category,
        p.sku,
        DATE_TRUNC('week', im.movement_date) as week,
        SUM(CASE WHEN im.movement_type = 'in' THEN im.quantity_moved ELSE 0 END) as quantity_in,
        SUM(CASE WHEN im.movement_type = 'out' THEN im.quantity_moved ELSE 0 END) as quantity_out,
        SUM(CASE WHEN im.movement_type = 'transfer' THEN im.quantity_moved ELSE 0 END) as quantity_transfer,
        COUNT(DISTINCT im.movement_id) as movement_count,
        AVG(im.unit_cost) as avg_unit_cost
    FROM products.products p
    JOIN inventory.inventory_movements im ON p.product_id = im.product_id
    WHERE im.movement_date >= CURRENT_DATE - INTERVAL '16 weeks'
    GROUP BY p.product_id, p.product_name, p.product_category, p.sku, DATE_TRUNC('week', im.movement_date)
),
inventory_forecast AS (
    SELECT 
        product_id,
        product_name,
        product_category,
        sku,
        week,
        quantity_in,
        quantity_out,
        quantity_transfer,
        movement_count,
        avg_unit_cost,
        -- Скользящее среднее потребления за 4 недели
        AVG(quantity_out) OVER (
            PARTITION BY product_id 
            ORDER BY week 
            ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        ) as avg_weekly_consumption,
        -- Тренд потребления (положительный = рост, отрицательный = снижение)
        quantity_out - LAG(quantity_out, 1) OVER (PARTITION BY product_id ORDER BY week) as consumption_trend,
        -- Прогноз на следующую неделю
        AVG(quantity_out) OVER (
            PARTITION BY product_id 
            ORDER BY week 
            ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        ) * 1.1 as forecast_next_week
    FROM inventory_trends
)
SELECT 
    product_name,
    product_category,
    sku,
    week,
    quantity_in,
    quantity_out,
    quantity_transfer,
    movement_count,
    avg_unit_cost,
    ROUND(avg_weekly_consumption, 2) as avg_weekly_consumption,
    consumption_trend,
    ROUND(forecast_next_week, 2) as forecast_next_week,
    CASE 
        WHEN consumption_trend > 0 THEN 'Growing Demand'
        WHEN consumption_trend < 0 THEN 'Declining Demand'
        ELSE 'Stable Demand'
    END as demand_trend,
    CASE 
        WHEN forecast_next_week > quantity_in * 0.8 THEN 'High Reorder Priority'
        WHEN forecast_next_week > quantity_in * 0.5 THEN 'Medium Reorder Priority'
        ELSE 'Low Reorder Priority'
    END as reorder_priority
FROM inventory_forecast
ORDER BY week DESC, reorder_priority DESC, forecast_next_week DESC;

-- 3. Анализ клиентского поведения и сегментация
-- Глубокий анализ клиентов по поведению и ценности
WITH customer_behavior AS (
    SELECT 
        c.customer_id,
        c.customer_name,
        c.customer_type,
        c.region,
        c.country,
        COUNT(DISTINCT o.order_id) as total_orders,
        SUM(oi.quantity_ordered * oi.unit_price) as total_spent,
        ROUND(AVG(oi.quantity_ordered * oi.unit_price), 2) as avg_order_value,
        MIN(o.order_date) as first_order_date,
        MAX(o.order_date) as last_order_date,
        ROUND(AVG(EXTRACT(EPOCH FROM (o.ship_date - o.order_date))/3600), 2) as avg_fulfillment_time_hours,
        COUNT(DISTINCT CASE WHEN o.status = 'completed' THEN o.order_id END) as completed_orders,
        COUNT(DISTINCT CASE WHEN o.status = 'cancelled' THEN o.order_id END) as cancelled_orders,
        COUNT(DISTINCT p.product_category) as product_categories_purchased,
        ROUND(
            COUNT(DISTINCT CASE WHEN o.status = 'completed' THEN o.order_id END)::numeric / 
            NULLIF(COUNT(DISTINCT o.order_id), 0) * 100, 2
        ) as order_completion_rate,
        EXTRACT(DAYS FROM (CURRENT_DATE - MIN(o.order_date))) as customer_lifetime_days,
        ROUND(SUM(oi.quantity_ordered * oi.unit_price) / NULLIF(EXTRACT(DAYS FROM (CURRENT_DATE - MIN(o.order_date))), 0), 2) as daily_spending_rate
    FROM customers.customers c
    LEFT JOIN orders.orders o ON c.customer_id = o.customer_id
    LEFT JOIN orders.order_items oi ON o.order_id = oi.order_id
    LEFT JOIN products.products p ON oi.product_id = p.product_id
    GROUP BY c.customer_id, c.customer_name, c.customer_type, c.region, c.country
),
customer_segmentation AS (
    SELECT 
        *,
        -- RFM анализ (Recency, Frequency, Monetary)
        CASE 
            WHEN last_order_date >= CURRENT_DATE - INTERVAL '30 days' THEN 5
            WHEN last_order_date >= CURRENT_DATE - INTERVAL '90 days' THEN 4
            WHEN last_order_date >= CURRENT_DATE - INTERVAL '180 days' THEN 3
            WHEN last_order_date >= CURRENT_DATE - INTERVAL '365 days' THEN 2
            ELSE 1
        END as recency_score,
        CASE 
            WHEN total_orders >= 50 THEN 5
            WHEN total_orders >= 20 THEN 4
            WHEN total_orders >= 10 THEN 3
            WHEN total_orders >= 5 THEN 2
            ELSE 1
        END as frequency_score,
        CASE 
            WHEN total_spent >= 10000 THEN 5
            WHEN total_spent >= 5000 THEN 4
            WHEN total_spent >= 1000 THEN 3
            WHEN total_spent >= 500 THEN 2
            ELSE 1
        END as monetary_score
    FROM customer_behavior
),
rfm_analysis AS (
    SELECT 
        *,
        (recency_score + frequency_score + monetary_score) as rfm_score,
        CASE 
            WHEN (recency_score + frequency_score + monetary_score) >= 13 THEN 'VIP Customers'
            WHEN (recency_score + frequency_score + monetary_score) >= 10 THEN 'High Value Customers'
            WHEN (recency_score + frequency_score + monetary_score) >= 7 THEN 'Medium Value Customers'
            WHEN (recency_score + frequency_score + monetary_score) >= 4 THEN 'Low Value Customers'
            ELSE 'At Risk Customers'
        END as rfm_segment,
        CASE 
            WHEN customer_lifetime_days >= 365 AND total_orders >= 20 THEN 'Loyal Long-term'
            WHEN customer_lifetime_days >= 180 AND total_orders >= 10 THEN 'Established'
            WHEN customer_lifetime_days >= 90 AND total_orders >= 5 THEN 'Growing'
            WHEN customer_lifetime_days >= 30 AND total_orders >= 2 THEN 'New Active'
            ELSE 'New'
        END as loyalty_segment
    FROM customer_segmentation
)
SELECT 
    customer_name,
    customer_type,
    region,
    country,
    total_orders,
    total_spent,
    avg_order_value,
    first_order_date,
    last_order_date,
    customer_lifetime_days,
    daily_spending_rate,
    order_completion_rate,
    product_categories_purchased,
    rfm_score,
    rfm_segment,
    loyalty_segment,
    CASE 
        WHEN rfm_segment = 'VIP Customers' THEN 'Premium Support, Exclusive Offers'
        WHEN rfm_segment = 'High Value Customers' THEN 'Priority Support, Special Discounts'
        WHEN rfm_segment = 'Medium Value Customers' THEN 'Standard Support, Regular Promotions'
        WHEN rfm_segment = 'Low Value Customers' THEN 'Basic Support, Entry-level Offers'
        ELSE 'Re-engagement Campaigns'
    END as recommended_action
FROM rfm_analysis
ORDER BY rfm_score DESC, total_spent DESC;

-- 4. Анализ эффективности операций по времени суток
-- Оптимизация операционных процессов на основе временных паттернов
WITH hourly_operations AS (
    SELECT 
        EXTRACT(HOUR FROM o.order_date) as hour_of_day,
        EXTRACT(DOW FROM o.order_date) as day_of_week,
        w.warehouse_id,
        w.warehouse_name,
        COUNT(DISTINCT o.order_id) as orders_count,
        SUM(oi.quantity_ordered) as total_quantity_ordered,
        ROUND(AVG(oi.quantity_ordered * oi.unit_price), 2) as avg_order_value,
        COUNT(DISTINCT o.customer_id) as unique_customers,
        ROUND(AVG(EXTRACT(EPOCH FROM (o.ship_date - o.order_date))/3600), 2) as avg_fulfillment_hours
    FROM orders.orders o
    JOIN orders.order_items oi ON o.order_id = oi.order_id
    JOIN warehouse.warehouses w ON o.warehouse_id = w.warehouse_id
    WHERE o.order_date >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY EXTRACT(HOUR FROM o.order_date), EXTRACT(DOW FROM o.order_date), w.warehouse_id, w.warehouse_name
),
hourly_efficiency AS (
    SELECT 
        *,
        CASE 
            WHEN hour_of_day BETWEEN 6 AND 9 THEN 'Early Morning'
            WHEN hour_of_day BETWEEN 9 AND 12 THEN 'Late Morning'
            WHEN hour_of_day BETWEEN 12 AND 14 THEN 'Lunch Time'
            WHEN hour_of_day BETWEEN 14 AND 17 THEN 'Afternoon'
            WHEN hour_of_day BETWEEN 17 AND 20 THEN 'Evening'
            WHEN hour_of_day BETWEEN 20 AND 23 THEN 'Late Evening'
            ELSE 'Night'
        END as time_period,
        CASE 
            WHEN day_of_week = 0 THEN 'Sunday'
            WHEN day_of_week = 1 THEN 'Monday'
            WHEN day_of_week = 2 THEN 'Tuesday'
            WHEN day_of_week = 3 THEN 'Wednesday'
            WHEN day_of_week = 4 THEN 'Thursday'
            WHEN day_of_week = 5 THEN 'Friday'
            ELSE 'Saturday'
        END as day_name,
        -- Эффективность по часам (больше заказов = выше эффективность)
        ROUND(
            orders_count::numeric / NULLIF(avg_fulfillment_hours, 0), 2
        ) as efficiency_score
    FROM hourly_operations
)
SELECT 
    warehouse_name,
    day_name,
    time_period,
    hour_of_day,
    orders_count,
    total_quantity_ordered,
    avg_order_value,
    unique_customers,
    avg_fulfillment_hours,
    efficiency_score,
    CASE 
        WHEN efficiency_score >= 10 THEN 'Very High'
        WHEN efficiency_score >= 7 THEN 'High'
        WHEN efficiency_score >= 4 THEN 'Medium'
        WHEN efficiency_score >= 1 THEN 'Low'
        ELSE 'Very Low'
    END as efficiency_level,
    CASE 
        WHEN orders_count >= 100 THEN 'Peak Hours - Max Staff'
        WHEN orders_count >= 50 THEN 'High Activity - Increased Staff'
        WHEN orders_count >= 20 THEN 'Moderate Activity - Standard Staff'
        WHEN orders_count >= 5 THEN 'Low Activity - Reduced Staff'
        ELSE 'Minimal Activity - Minimal Staff'
    END as staffing_recommendation
FROM hourly_efficiency
ORDER BY warehouse_name, day_of_week, hour_of_day;

-- 5. Анализ цепочки поставок и логистики
-- Оптимизация логистических процессов и выявление узких мест
WITH supply_chain_analysis AS (
    SELECT 
        DATE_TRUNC('day', r.receive_date) as receive_date,
        DATE_TRUNC('day', s.ship_date) as ship_date,
        w.warehouse_id,
        w.warehouse_name,
        w.region,
        -- Получение товаров
        COUNT(DISTINCT r.receipt_id) as receipts_count,
        SUM(ri.quantity_received) as total_quantity_received,
        ROUND(AVG(EXTRACT(EPOCH FROM (r.receive_date - r.created_date))/3600), 2) as avg_receiving_time_hours,
        -- Отгрузка товаров
        COUNT(DISTINCT s.shipment_id) as shipments_count,
        SUM(si.quantity_shipped) as total_quantity_shipped,
        ROUND(AVG(EXTRACT(EPOCH FROM (s.ship_date - s.created_date))/3600), 2) as avg_shipping_time_hours,
        -- Время обработки (от получения до отгрузки)
        ROUND(AVG(EXTRACT(EPOCH FROM (s.ship_date - r.receive_date))/3600), 2) as avg_processing_time_hours
    FROM warehouse.warehouses w
    LEFT JOIN receiving.receipts r ON w.warehouse_id = r.warehouse_id
    LEFT JOIN receiving.receipt_items ri ON r.receipt_id = ri.receipt_id
    LEFT JOIN shipping.shipments s ON w.warehouse_id = s.warehouse_id
    LEFT JOIN shipping.shipment_items si ON s.shipment_id = si.shipment_id
    WHERE (r.receive_date >= CURRENT_DATE - INTERVAL '30 days' OR s.ship_date >= CURRENT_DATE - INTERVAL '30 days')
    GROUP BY DATE_TRUNC('day', r.receive_date), DATE_TRUNC('day', s.ship_date), w.warehouse_id, w.warehouse_name, w.region
),
supply_chain_metrics AS (
    SELECT 
        *,
        -- Эффективность обработки
        CASE 
            WHEN avg_processing_time_hours <= 2 THEN 'Excellent'
            WHEN avg_processing_time_hours <= 4 THEN 'Good'
            WHEN avg_processing_time_hours <= 8 THEN 'Average'
            WHEN avg_processing_time_hours <= 24 THEN 'Poor'
            ELSE 'Very Poor'
        END as processing_efficiency,
        -- Соотношение входящих и исходящих потоков
        CASE 
            WHEN total_quantity_received > 0 AND total_quantity_shipped > 0 THEN
                ROUND((total_quantity_shipped::numeric / total_quantity_received) * 100, 2)
            ELSE 0
        END as throughput_percentage,
        -- Общая эффективность склада
        CASE 
            WHEN avg_receiving_time_hours <= 1 AND avg_shipping_time_hours <= 2 AND avg_processing_time_hours <= 4 THEN 'High Performance'
            WHEN avg_receiving_time_hours <= 2 AND avg_shipping_time_hours <= 4 AND avg_processing_time_hours <= 8 THEN 'Good Performance'
            WHEN avg_receiving_time_hours <= 4 AND avg_shipping_time_hours <= 8 AND avg_processing_time_hours <= 16 THEN 'Average Performance'
            ELSE 'Needs Improvement'
        END as overall_performance
    FROM supply_chain_analysis
)
SELECT 
    warehouse_name,
    region,
    receive_date,
    ship_date,
    receipts_count,
    total_quantity_received,
    avg_receiving_time_hours,
    shipments_count,
    total_quantity_shipped,
    avg_shipping_time_hours,
    avg_processing_time_hours,
    processing_efficiency,
    throughput_percentage,
    overall_performance,
    CASE 
        WHEN avg_receiving_time_hours > 4 THEN 'Receiving bottleneck - Add staff/equipment'
        WHEN avg_shipping_time_hours > 8 THEN 'Shipping bottleneck - Optimize picking process'
        WHEN avg_processing_time_hours > 16 THEN 'Processing bottleneck - Review workflow'
        ELSE 'No immediate bottlenecks detected'
    END as bottleneck_analysis,
    CASE 
        WHEN overall_performance = 'High Performance' THEN 'Maintain current processes'
        WHEN overall_performance = 'Good Performance' THEN 'Minor optimizations recommended'
        WHEN overall_performance = 'Average Performance' THEN 'Process review and improvements needed'
        ELSE 'Major process redesign required'
    END as improvement_recommendation
FROM supply_chain_metrics
ORDER BY receive_date DESC, warehouse_name;
