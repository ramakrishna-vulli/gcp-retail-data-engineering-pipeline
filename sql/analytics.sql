-- ============================================================
-- GCP RETAIL DATA ENGINEERING PIPELINE
-- BigQuery Analytics Queries
-- ============================================================

-- Project:
-- vast-falcon-415411
--
-- Dataset:
-- retail_analytics


-- ============================================================
-- 1. Overall Sales Summary
-- ============================================================

SELECT
    COUNT(*) AS total_orders,
    SUM(quantity) AS total_quantity,
    SUM(gross_sales) AS gross_sales,
    SUM(discount_amount) AS total_discount,
    SUM(net_sales) AS net_sales
FROM `vast-falcon-415411.retail_analytics.sales`;


-- ============================================================
-- 2. Daily Sales Performance
-- ============================================================

SELECT
    order_date,
    order_count,
    total_quantity,
    gross_sales,
    total_discount,
    net_sales
FROM `vast-falcon-415411.retail_analytics.daily_sales`
ORDER BY order_date;


-- ============================================================
-- 3. Category Performance
-- ============================================================

SELECT
    category,
    order_count,
    total_quantity,
    net_sales
FROM `vast-falcon-415411.retail_analytics.category_sales`
ORDER BY net_sales DESC;


-- ============================================================
-- 4. Store Performance
-- ============================================================

SELECT
    store_id,
    order_count,
    total_quantity,
    net_sales
FROM `vast-falcon-415411.retail_analytics.store_sales`
ORDER BY net_sales DESC;


-- ============================================================
-- 5. Payment Method Performance
-- ============================================================

SELECT
    payment_method,
    order_count,
    net_sales
FROM `vast-falcon-415411.retail_analytics.payment_method_sales`
ORDER BY net_sales DESC;


-- ============================================================
-- 6. Top 5 Orders by Net Sales
-- ============================================================

SELECT
    order_id,
    order_date,
    category,
    quantity,
    gross_sales,
    discount_amount,
    net_sales,
    store_id,
    payment_method
FROM `vast-falcon-415411.retail_analytics.sales`
ORDER BY net_sales DESC
LIMIT 5;


-- ============================================================
-- 7. Category Sales Percentage
-- ============================================================

SELECT
    category,
    net_sales,
    ROUND(
        SAFE_DIVIDE(
            net_sales,
            SUM(net_sales) OVER ()
        ) * 100,
        2
    ) AS sales_percentage
FROM `vast-falcon-415411.retail_analytics.category_sales`
ORDER BY net_sales DESC;


-- ============================================================
-- 8. Discount Analysis
-- ============================================================

SELECT
    category,
    SUM(gross_sales) AS gross_sales,
    SUM(discount_amount) AS total_discount,
    SUM(net_sales) AS net_sales,
    ROUND(
        SAFE_DIVIDE(
            SUM(discount_amount),
            SUM(gross_sales)
        ) * 100,
        2
    ) AS discount_percentage
FROM `vast-falcon-415411.retail_analytics.sales`
GROUP BY category
ORDER BY total_discount DESC;