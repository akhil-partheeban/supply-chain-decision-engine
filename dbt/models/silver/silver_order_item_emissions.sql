-- Estimated Scope 3 (purchased-goods) GHG emissions, one row per order item — same
-- grain as silver_order_items, which this enriches. Spend-based method: convert the
-- item's BRL price to a 2022-USD-equivalent, then multiply by the EPA emission
-- factor for that item's mapped NAICS commodity. See DECISIONS.md, Phase 8, for why
-- spend-based (not activity-based) and the full list of limitations this approach
-- carries.
--
-- Items whose product_category has no row in category_to_naics (including items
-- with a NULL category — ~1.9% of sellers have no resolvable category at all; see
-- the gold_supplier_emissions header) simply get a NULL factor and NULL
-- estimated_kg_co2e here — not a fabricated 0. gold_supplier_emissions is explicit
-- about how much of each seller's spend this covers (pct_spend_mapped).

WITH converted AS (
    SELECT
        oi.order_id,
        oi.order_item_id,
        oi.seller_id,
        oi.product_category,
        oi.price AS price_brl,
        -- BRL (nominal, mostly 2017-2018) -> 2022-USD-equivalent, in two documented
        -- steps (see dbt_project.yml vars block for both sources):
        --   1. BRL -> nominal USD at the 2018 average FX rate
        --   2. nominal USD -> 2022 USD via the BLS CPI-U adjustment factor
        round(
            oi.price * {{ var('brl_to_usd_rate') }} * {{ var('cpi_2018_to_2022_adjustment') }},
        4) AS spend_usd_2022
    FROM {{ ref('silver_order_items') }} oi
)

SELECT
    c.order_id,
    c.order_item_id,
    c.seller_id,
    c.product_category,
    c.price_brl,
    c.spend_usd_2022,
    m.naics_code,
    f.factor_with_margins_kg_co2e_per_usd,
    round(c.spend_usd_2022 * f.factor_with_margins_kg_co2e_per_usd, 4) AS estimated_kg_co2e
FROM converted c
LEFT JOIN {{ ref('category_to_naics') }} m
    ON c.product_category = m.product_category
LEFT JOIN {{ ref('epa_ghg_emission_factors') }} f
    ON m.naics_code = f.naics_code
