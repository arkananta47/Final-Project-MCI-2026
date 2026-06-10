-- MCI2026 Final Project — DustiniaDelixia Groceria 
-- Database: fpmci2026_db

-- Q1: Executive KPI Overview
SELECT
    total_orders,
    ROUND(avg_delay_days,2) AS avg_delay_days,
    ROUND(late_rate * 100,2) AS late_rate_percent,
    ROUND(avg_review_score,2) AS avg_review_score
FROM fpmci2026_db.kpi_summary;


-- Q2: On-Time vs Late Delivery Rate
SELECT
    CASE
        WHEN is_late = 1 THEN 'Late'
        ELSE 'On-Time'
    END AS delivery_status,
    COUNT(*) AS total_orders
FROM fpmci2026_db.fact_operational_orders
GROUP BY delivery_status


-- Q3: Top 10 State dengan Keterlambatan Terbesar
SELECT
    customer_state,
    total_orders,
    ROUND(avg_delay_days,2) AS avg_delay_days,
    ROUND(late_rate * 100,2) AS late_rate_percent
FROM fpmci2026_db.state_summary
ORDER BY avg_delay_days DESC
LIMIT 10;


-- Q4: State dengan Kepuasan Terendah
SELECT
    customer_state,
    ROUND(avg_review_score,2) AS avg_review_score,
    total_orders
FROM fpmci2026_db.state_summary
ORDER BY avg_review_score ASC
LIMIT 10;


-- Q5: Top Seller dengan SLA Breach Tertinggi
SELECT
    seller_id,
    total_orders,
    ROUND(sla_breach_rate * 100,2) AS sla_breach_percent,
    ROUND(avg_delay_days,2) AS avg_delay_days
FROM fpmci2026_db.seller_summary
WHERE total_orders > 50
ORDER BY sla_breach_rate DESC
LIMIT 15;


-- Q6: Top 10 Seller Terbaik
SELECT
    seller_id,
    total_orders,
    ROUND(avg_review_score,2) AS avg_review_score,
    ROUND(sla_breach_rate * 100,2) AS sla_breach_percent
FROM fpmci2026_db.seller_summary
WHERE total_orders > 50
ORDER BY avg_review_score DESC
LIMIT 15;


-- Q7: Monthly Delivery Trend
SELECT
    order_month,
    total_orders,
    ROUND(avg_delay_days,2) AS avg_delay_days,
    ROUND(late_rate * 100,2) AS late_rate_percent
FROM fpmci2026_db.monthly_trend
ORDER BY order_month;


-- Q8: Sentiment Distribution
SELECT
    sentiment_label,
    COUNT(*) AS total_reviews,
    ROUND(
        COUNT(*) * 100.0 /
        SUM(COUNT(*)) OVER (),
        2
    ) AS percentage
FROM fpmci2026_db.fact_review_sentiment
GROUP BY sentiment_label;


-- Q9: Sentiment vs Delay
SELECT
    sentiment_label,

    ROUND(
        AVG(delivery_delay_days),
        2
    ) AS avg_delay_days,

    COUNT(*) AS total_reviews

FROM fpmci2026_db.fact_review_sentiment
GROUP BY sentiment_label
ORDER BY avg_delay_days DESC;


-- Q10: Review Score Distribution
SELECT
    review_score,
    COUNT(*) AS total_reviews
FROM fpmci2026_db.fact_operational_orders
GROUP BY review_score
ORDER BY review_score;
