-- Cleaned orders with parsed timestamps and derived delivery metrics.
-- Grain: one row per order_id. No cross-table joins here — aggregation happens in gold.
SELECT
    order_id,
    customer_id,
    order_status,
    CAST(order_purchase_timestamp AS TIMESTAMP)     AS purchased_at,
    CAST(order_approved_at AS TIMESTAMP)            AS approved_at,
    CAST(order_delivered_carrier_date AS TIMESTAMP) AS carrier_pickup_at,
    CAST(order_delivered_customer_date AS TIMESTAMP) AS delivered_at,
    CAST(order_estimated_delivery_date AS TIMESTAMP) AS estimated_delivery_at,
    date_diff(
        'day',
        CAST(order_purchase_timestamp AS TIMESTAMP),
        CAST(order_delivered_customer_date AS TIMESTAMP)
    )                                               AS actual_delivery_days,
    date_diff(
        'day',
        CAST(order_purchase_timestamp AS TIMESTAMP),
        CAST(order_estimated_delivery_date AS TIMESTAMP)
    )                                               AS promised_delivery_days,
    -- Delivered flag uses the actual delivery timestamp, not order_status: a handful of
    -- orders carry a "delivered" status with a null delivery date (data-quality quirk
    -- in the source Olist extract), which would otherwise poison delivery-day averages.
    CASE WHEN order_delivered_customer_date IS NOT NULL THEN 1 ELSE 0 END AS is_delivered,
    CASE
        WHEN order_delivered_customer_date IS NOT NULL
         AND order_delivered_customer_date > order_estimated_delivery_date
        THEN 1 ELSE 0
    END                                               AS is_late,
    _source_file,
    _loaded_at
FROM {{ source('bronze', 'orders') }}
WHERE order_id IS NOT NULL
