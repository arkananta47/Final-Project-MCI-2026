-- MCI2026 Final Project — DustiniaDelixia Groceria 
-- Database: fpmci2026_db

-- Q1: Daily Order Activity
SELECT
    order_date,
    total_orders,
    total_items
FROM mci2026_db.orders_daily_orders
ORDER BY order_date ASC;


-- Q2: Top 30 Most Ordered Products
SELECT
    product_name,
    department,
    total_orders,
    total_items,
    total_reordered
FROM mci2026_db.orders_trending_products
ORDER BY total_items DESC
LIMIT 30;


-- Q3: Most Popular Departments
SELECT
    department,
    total_orders,
    total_items_sold,
    round(reorder_rate * 100, 2)
        AS reorder_rate_percent
FROM mci2026_db.orders_category_summary
ORDER BY total_items_sold DESC;


-- Q4: Reorder Distribution
SELECT
    CASE
        WHEN reordered = 1
            THEN 'Reordered'
        ELSE 'First Purchase'
    END AS reorder_status,
    count() AS total_items,
    round(
        count() * 100.0 /
        sum(count()) OVER (),
        2
    ) AS percentage
FROM mci2026_db.orders
GROUP BY reorder_status
ORDER BY total_items DESC;


-- Q5: Top 10 Most Active Users
SELECT
    user_id,
    countDistinct(order_id)
        AS total_orders,
    count()
        AS total_items,
    avg(add_to_cart_order)
        AS avg_cart_position
FROM mci2026_db.orders
GROUP BY user_id
ORDER BY total_items DESC
LIMIT 10;


-- Q6: Most Popular Aisles
SELECT
    aisle,
    count()
        AS total_items,
    countDistinct(order_id)
        AS total_orders
FROM mci2026_db.orders
WHERE aisle != ''
GROUP BY aisle
ORDER BY total_items DESC
LIMIT 15;


-- Q7: Basket Size Distribution
SELECT
    CASE
        WHEN item_count <= 5
            THEN '1-5 Items'
        WHEN item_count <= 10
            THEN '6-10 Items'
        WHEN item_count <= 20
            THEN '11-20 Items'
        ELSE '20+ Items'
    END AS basket_size,
    count() AS total_orders
FROM (
    SELECT
        order_id,
        count() AS item_count
    FROM mci2026_db.orders
    GROUP BY order_id
)
GROUP BY basket_size
ORDER BY total_orders DESC;


-- Q8: Top Reordered Products
SELECT
    product_name,
    department,
    sum(reordered)
        AS total_reordered,
    count()
        AS total_orders,
    round(
        sum(reordered) * 100.0 / count(),
        2
    ) AS reorder_rate_pct
FROM mci2026_db.orders
GROUP BY product_name, department
HAVING total_orders >= 5
ORDER BY reorder_rate_pct DESC
LIMIT 20;


-- Q9: KPI Summary Dashboard
SELECT
    countDistinct(order_id)
        AS total_orders,
    countDistinct(user_id)
        AS unique_users,
    countDistinct(product_id)
        AS unique_products,
    countDistinct(department)
        AS unique_departments,
    count()
        AS total_items_sold,
    round(
        avg(reordered) * 100,
        2
    ) AS overall_reorder_rate_pct
FROM mci2026_db.orders;


-- Q10: Shopping Time Analysis
SELECT
    order_hour_of_day,
    countDistinct(order_id)
        AS total_orders,
    count()
        AS total_items
FROM mci2026_db.orders
GROUP BY order_hour_of_day
ORDER BY order_hour_of_day ASC;