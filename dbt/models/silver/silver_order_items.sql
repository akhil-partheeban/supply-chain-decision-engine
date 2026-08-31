-- Order-line grain: one row per (order_id, order_item_id). Enriches bronze order_items
-- with product category (English) and weight so gold can compute sourcing cost drivers
-- without re-joining products in every downstream model.
SELECT
    oi.order_id,
    oi.order_item_id,
    oi.product_id,
    oi.seller_id,
    oi.price,
    oi.freight_value,
    round(oi.freight_value / nullif(oi.price, 0) * 100, 2)         AS freight_pct_of_price,
    p.product_weight_g,
    -- freight cost per kg shipped: normalizes freight spend by physical weight so
    -- heavy-but-cheap items aren't mistaken for "expensive to ship" categories
    round(oi.freight_value / nullif(p.product_weight_g / 1000.0, 0), 4) AS freight_cost_per_kg,
    coalesce(t.product_category_name_english, p.product_category_name) AS product_category
FROM {{ source('bronze', 'order_items') }} oi
LEFT JOIN {{ source('bronze', 'products') }} p
    ON oi.product_id = p.product_id
LEFT JOIN {{ source('bronze', 'product_category_name_translation') }} t
    ON p.product_category_name = t.product_category_name
