-- Single-row portfolio rollup for the dashboard's top KPI strip. Deliberately a single
-- row (not a table you'd query for detail) — it exists purely to answer "how is the
-- supply base doing, overall" in one glance.

WITH scorecard_agg AS (
    SELECT
        sum(total_orders)                                                   AS total_orders,
        count(DISTINCT seller_id)                                           AS total_suppliers,
        sum(unique_products)                                                AS total_product_listings,
        round(sum(total_orders * late_delivery_rate) / nullif(sum(total_orders), 0), 4)
                                                                              AS overall_late_rate,
        round(avg(reliability_score), 1)                                    AS avg_reliability_score,
        round(
            count(*) FILTER (WHERE risk_tier = 'HIGH') / nullif(count(*), 0)::DOUBLE, 4
        )                                                                    AS pct_high_risk_suppliers,
        round(sum(review_count * avg_review_score) / nullif(sum(review_count), 0), 4)
                                                                              AS avg_review_score,
        round(sum(total_revenue), 2)                                        AS total_revenue
    FROM {{ ref('gold_supplier_scorecard') }}
),

concentration_agg AS (
    SELECT
        round(sum(hhi_contribution), 2)                                     AS hhi_index,
        round(sum(revenue_share_pct) FILTER (WHERE revenue_rank <= 5), 2)   AS top5_supplier_revenue_share_pct
    FROM {{ ref('gold_concentration_risk') }}
),

cost_agg AS (
    SELECT count(*) AS categories_scored
    FROM {{ ref('gold_sourcing_cost_drivers') }}
)

SELECT
    sa.total_orders,
    sa.total_suppliers,
    sa.total_product_listings,
    sa.total_revenue,
    sa.overall_late_rate,
    sa.avg_reliability_score,
    sa.pct_high_risk_suppliers,
    sa.avg_review_score,
    ca.hhi_index,
    -- DOJ/FTC merger-guideline bands, reused here for supply-base concentration
    -- (see gold_concentration_risk header for the full rationale).
    CASE
        WHEN ca.hhi_index >= 2500 THEN 'HIGHLY_CONCENTRATED'
        WHEN ca.hhi_index >= 1500 THEN 'MODERATELY_CONCENTRATED'
        ELSE 'UNCONCENTRATED'
    END AS hhi_interpretation,
    ca.top5_supplier_revenue_share_pct,
    co.categories_scored
FROM scorecard_agg sa
CROSS JOIN concentration_agg ca
CROSS JOIN cost_agg co
