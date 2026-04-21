-- Cleaned orders with parsed timestamps and derived delivery metrics
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
    _source_file,
    _loaded_at
FROM {{ source('bronze', 'orders') }}
WHERE order_id IS NOT NULL
