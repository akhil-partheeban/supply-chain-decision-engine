-- Aggregated supplier / seller performance scorecard
SELECT
    s.seller_id,
    s.seller_city,
    s.seller_state,
    count(DISTINCT oi.order_id)                         AS total_orders,
    count(DISTINCT oi.product_id)                       AS unique_products,
    round(avg(o.actual_delivery_days), 2)               AS avg_delivery_days,
    round(avg(r.review_score), 2)                       AS avg_review_score,
    sum(oi.price)                                       AS total_revenue,
    sum(oi.freight_value)                               AS total_freight_cost,
    round(sum(oi.freight_value) / nullif(sum(oi.price), 0) * 100, 2) AS freight_pct_of_revenue
FROM {{ source('bronze', 'sellers') }} s
JOIN {{ source('bronze', 'order_items') }} oi  USING (seller_id)
JOIN {{ ref('silver_orders') }} o              USING (order_id)
LEFT JOIN {{ source('bronze', 'order_reviews') }} r USING (order_id)
GROUP BY 1, 2, 3
