-- Seller-level metrics aggregated from bronze order data
WITH seller_orders AS (
    SELECT
        oi.seller_id,
        o.order_id,
        o.order_status,
        o.order_delivered_customer_date AS delivered_at,
        o.order_estimated_delivery_date AS estimated_delivery_at,
        date_diff(
            'day',
            CAST(o.order_purchase_timestamp AS TIMESTAMP),
            CAST(o.order_delivered_customer_date AS TIMESTAMP)
        ) AS delivery_days,
        CASE
            WHEN o.order_delivered_customer_date > o.order_estimated_delivery_date THEN 1
            ELSE 0
        END AS is_late
    FROM {{ source('bronze', 'order_items') }} oi
    JOIN {{ source('bronze', 'orders') }} o ON oi.order_id = o.order_id
    WHERE oi.seller_id IS NOT NULL
),

seller_reviews AS (
    SELECT
        oi.seller_id,
        AVG(CAST(r.review_score AS DOUBLE)) AS avg_review_score
    FROM {{ source('bronze', 'order_items') }} oi
    JOIN {{ source('bronze', 'order_reviews') }} r ON oi.order_id = r.order_id
    WHERE oi.seller_id IS NOT NULL
    GROUP BY oi.seller_id
)

SELECT
    s.seller_id,
    s.seller_city,
    s.seller_state,
    COUNT(DISTINCT so.order_id)                     AS total_orders,
    AVG(CAST(so.is_late AS DOUBLE))                 AS late_delivery_rate,
    AVG(so.delivery_days)                           AS avg_delivery_days,
    sr.avg_review_score
FROM {{ source('bronze', 'sellers') }} s
LEFT JOIN seller_orders so ON s.seller_id = so.seller_id
LEFT JOIN seller_reviews sr ON s.seller_id = sr.seller_id
GROUP BY s.seller_id, s.seller_city, s.seller_state, sr.avg_review_score
