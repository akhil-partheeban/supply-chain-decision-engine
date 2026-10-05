-- Category-level emissions hotspot: which product categories contribute the most
-- to total estimated spend-based Scope 3 emissions, and is that because of volume
-- (lots of spend) or category-level carbon intensity (a high factor per dollar),
-- or both? This is the level of aggregation a spend-based estimate is actually
-- good for — see DECISIONS.md, Phase 9, for why a per-supplier "switch to this
-- other seller" comparison (the deleted gold_supplier_emission_swap_suggestions)
-- was not.
--
-- Grain: one row per product_category. Aggregated straight from
-- silver_order_item_emissions (order-item grain), NOT from gold_supplier_emissions'
-- per-seller primary_category — a multi-category seller's spend in their
-- *secondary* categories would be invisible to this hotspot view if grouped by
-- primary_category instead, undercounting exactly the categories that matter.
--
-- category_emissions_intensity here is mathematically just that category's EPA
-- factor (every item in a category shares one NAICS factor by construction — see
-- category_to_naics), shown for reference, not as a derived insight. The real
-- signal in this table is total_estimated_kg_co2e and pct_of_portfolio_emissions:
-- a category can be a hotspot from high volume at an ordinary factor, an ordinary
-- volume at a high factor, or both — this table doesn't collapse that distinction
-- into a single score, on purpose.
--
-- No minimum-item-count filter (unlike gold_sourcing_cost_drivers' >=30 threshold)
-- — that threshold exists there to keep an *average* from being noisy on small N;
-- here every figure is a SUM, which is just as accurate for a long-tail category
-- with a handful of items as for a category with thousands.

WITH category_agg AS (
    SELECT
        product_category,
        count(*)                                                        AS total_items,
        count(DISTINCT seller_id)                                       AS seller_count,
        round(sum(spend_usd_2022), 4)                                   AS total_spend_usd,
        round(sum(estimated_kg_co2e), 4)                                AS total_estimated_kg_co2e
    FROM {{ ref('silver_order_item_emissions') }}
    WHERE product_category IS NOT NULL
      AND estimated_kg_co2e IS NOT NULL
    GROUP BY product_category
),

totals AS (
    SELECT sum(total_estimated_kg_co2e) AS portfolio_total_kg_co2e
    FROM category_agg
)

SELECT
    ca.product_category,
    ca.total_items,
    ca.seller_count,
    ca.total_spend_usd,
    ca.total_estimated_kg_co2e,
    round(ca.total_estimated_kg_co2e / nullif(ca.total_spend_usd, 0), 6) AS category_emissions_intensity,
    round(ca.total_estimated_kg_co2e / nullif(t.portfolio_total_kg_co2e, 0) * 100, 2) AS pct_of_portfolio_emissions,
    row_number() OVER (ORDER BY ca.total_estimated_kg_co2e DESC) AS emissions_rank
FROM category_agg ca
CROSS JOIN totals t
ORDER BY ca.total_estimated_kg_co2e DESC
