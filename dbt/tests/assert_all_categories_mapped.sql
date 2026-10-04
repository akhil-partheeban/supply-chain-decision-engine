-- Fails if any product_category that actually appears in silver_order_items has
-- no row in category_to_naics. NULL categories are excluded deliberately — they
-- represent order items whose product_id never joined to the products table at
-- all (no category of any kind to map), a separate, already-documented gap
-- (see gold_supplier_emissions.pct_spend_mapped), not a missing mapping row.
SELECT DISTINCT oi.product_category
FROM {{ ref('silver_order_items') }} oi
LEFT JOIN {{ ref('category_to_naics') }} m
    ON oi.product_category = m.product_category
WHERE oi.product_category IS NOT NULL
  AND m.product_category IS NULL
