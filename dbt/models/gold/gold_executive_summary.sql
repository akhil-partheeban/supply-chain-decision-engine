WITH seller_stats AS (
    SELECT
        COUNT(DISTINCT seller_id)                                   AS total_sellers,
        SUM(total_orders)                                           AS total_orders,
        SUM(total_orders * late_delivery_rate) / NULLIF(SUM(total_orders), 0) AS overall_late_rate,
        ROUND(AVG(avg_review_score), 4)                             AS avg_review_score,
        ROUND(
            COUNT(DISTINCT CASE WHEN late_delivery_rate > 0.3 THEN seller_id END)
            / NULLIF(COUNT(DISTINCT seller_id), 0)::DOUBLE,
            4
        )                                                           AS pct_high_risk_sellers
    FROM {{ ref('silver_sellers') }}
    WHERE total_orders > 0
)

SELECT
    total_orders,
    total_sellers,
    ROUND(overall_late_rate, 4) AS overall_late_rate,
    pct_high_risk_sellers,
    avg_review_score
FROM seller_stats
