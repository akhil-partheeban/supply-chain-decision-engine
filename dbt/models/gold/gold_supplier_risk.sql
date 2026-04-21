SELECT
    seller_id,
    seller_city,
    seller_state,
    total_orders,
    ROUND(late_delivery_rate, 4)  AS late_delivery_rate,
    ROUND(avg_review_score, 4)    AS avg_review_score,
    ROUND(avg_delivery_days, 2)   AS avg_delivery_days,
    CASE
        WHEN late_delivery_rate > 0.3  THEN 'HIGH'
        WHEN late_delivery_rate > 0.15 THEN 'MEDIUM'
        ELSE                                'LOW'
    END                           AS risk_tier
FROM {{ ref('silver_sellers') }}
WHERE total_orders > 0
