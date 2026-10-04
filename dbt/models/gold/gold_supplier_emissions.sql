-- Per-supplier estimated Scope 3 (purchased-goods) emissions, rolled up from
-- silver_order_item_emissions. Grain: one row per seller_id, same universe as
-- gold_supplier_scorecard (every seller with >=1 order item), so this joins
-- cleanly against it in gold_supplier_risk_emissions_score.
--
-- total_spend_usd is spend across ALL of a seller's order items (comparable to
-- gold_supplier_scorecard.total_revenue, just currency-converted); mapped_spend_usd
-- is the subset whose product_category actually resolved to a NAICS factor.
-- pct_spend_mapped makes the gap visible rather than silent — ~1.9% of sellers
-- (60 of 3,095) have zero resolvable categories (see silver_order_item_emissions)
-- and get pct_spend_mapped = 0 and emissions_intensity = NULL here, not a
-- fabricated 0. A NULL intensity means "we have no basis to estimate this
-- seller's emissions," not "this seller has zero emissions."

WITH item_level AS (
    SELECT *
    FROM {{ ref('silver_order_item_emissions') }}
    WHERE seller_id IS NOT NULL
),

-- Highest-spend category per seller, by the same 2022-USD-equivalent spend used
-- everywhere else in this model (not raw BRL) — only over categories that resolved
-- to a real name (excludes NULL-category items, which can't define a "primary
-- category" for anyone).
category_spend AS (
    SELECT seller_id, product_category, sum(spend_usd_2022) AS category_spend_usd
    FROM item_level
    WHERE product_category IS NOT NULL
    GROUP BY 1, 2
),

primary_category AS (
    SELECT seller_id, product_category AS primary_category
    FROM (
        SELECT
            seller_id,
            product_category,
            -- Tiebreak alphabetically for determinism on an exact spend tie —
            -- rare, but without it row order would be undefined.
            row_number() OVER (
                PARTITION BY seller_id ORDER BY category_spend_usd DESC, product_category ASC
            ) AS rn
        FROM category_spend
    )
    WHERE rn = 1
),

seller_agg AS (
    SELECT
        seller_id,
        round(sum(spend_usd_2022), 4)                                              AS total_spend_usd,
        round(sum(spend_usd_2022) FILTER (WHERE estimated_kg_co2e IS NOT NULL), 4)  AS mapped_spend_usd,
        round(sum(estimated_kg_co2e), 4)                                            AS estimated_scope3_kg_co2e
    FROM item_level
    GROUP BY seller_id
),

scored AS (
    SELECT
        sa.seller_id,
        sa.total_spend_usd,
        sa.mapped_spend_usd,
        round(sa.mapped_spend_usd / nullif(sa.total_spend_usd, 0), 4)            AS pct_spend_mapped,
        sa.estimated_scope3_kg_co2e,
        -- kg CO2e per mapped USD of spend. NULL (not 0) when mapped_spend_usd = 0
        -- — see header. This is the figure gold_supplier_risk_emissions_score and
        -- the swap-suggestions model both key off of.
        round(sa.estimated_scope3_kg_co2e / nullif(sa.mapped_spend_usd, 0), 6)   AS emissions_intensity,
        pc.primary_category
    FROM seller_agg sa
    LEFT JOIN primary_category pc USING (seller_id)
)

SELECT
    *,
    -- Rank within primary_category, lowest intensity first (1 = cleanest supplier
    -- in that category) — this is what the swap-suggestions model searches for.
    -- Sellers with no mapped category at all share one meaningless NULL
    -- "category" partition, ranked last by NULLS LAST; they're never eligible as
    -- a swap alternative (gold_supplier_emission_swap_suggestions requires a real
    -- primary_category to match on).
    row_number() OVER (
        PARTITION BY primary_category ORDER BY emissions_intensity ASC NULLS LAST
    ) AS rank_in_category
FROM scored
