-- Sourcing cost driver analysis, by product category: where is freight eating into
-- landed cost, and is that a function of price (cheap items make any freight look
-- expensive) or physical weight (bulky items are genuinely expensive to ship)?
--
-- Grain: one row per product category. Categories with too few line items are excluded
-- (see min_items filter) because a handful of orders can produce a wildly noisy average.

WITH category_agg AS (
    SELECT
        product_category,
        count(*)                                      AS total_items,
        count(DISTINCT seller_id)                      AS seller_count,
        round(sum(price), 2)                           AS total_spend,
        round(sum(freight_value), 2)                   AS total_freight_cost,
        round(avg(price), 2)                           AS avg_item_price,
        round(avg(freight_value), 2)                   AS avg_freight_value,
        round(avg(freight_cost_per_kg), 4)              AS avg_freight_cost_per_kg
    FROM {{ ref('silver_order_items') }}
    WHERE product_category IS NOT NULL
    GROUP BY product_category
    -- Below 30 line items, avg_price/avg_freight are too noisy to act on — matches the
    -- materiality bar used to derive the freight_pct_of_price quartiles below.
    HAVING count(*) >= 30
)

SELECT
    product_category,
    total_items,
    seller_count,
    total_spend,
    total_freight_cost,
    avg_item_price,
    avg_freight_value,
    avg_freight_cost_per_kg,
    round(total_freight_cost / nullif(total_spend, 0) * 100, 2) AS freight_pct_of_spend,
    -- Tiers grounded in the actual distribution of freight_pct_of_spend across the 67
    -- categories that clear the >=30-item materiality bar: median ~18.5%, p75 ~22.8%.
    -- HIGH (>25%) sits just above the top quartile; MEDIUM covers "worse than typical."
    CASE
        WHEN total_freight_cost / nullif(total_spend, 0) * 100 > 25   THEN 'HIGH'
        WHEN total_freight_cost / nullif(total_spend, 0) * 100 > 18.5 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS freight_burden_tier
FROM category_agg
ORDER BY freight_pct_of_spend DESC
