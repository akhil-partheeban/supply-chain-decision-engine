-- Supplier concentration risk: how dependent is total sourcing spend on any single seller?
-- Grain: one row per seller, ranked by revenue share, with each seller's contribution to
-- the portfolio-wide Herfindahl-Hirschman Index (HHI) — the standard economics measure of
-- market concentration, repurposed here to measure supply-base concentration.
--
-- HHI = sum of (market share in percentage points)^2, so it ranges 0 (infinitely many
-- equal-share sellers) to 10,000 (single seller holds 100%). Interpretation bands follow
-- the US DOJ/FTC Horizontal Merger Guidelines convention (the standard reference point for
-- this index): <1,500 unconcentrated, 1,500-2,500 moderately concentrated, >2,500 highly
-- concentrated. See gold_executive_summary for the portfolio-level HHI total and
-- DECISIONS.md for why that number comes out very low for this dataset.

WITH seller_revenue AS (
    SELECT
        seller_id,
        round(sum(price), 2) AS total_revenue
    FROM {{ ref('silver_order_items') }}
    WHERE seller_id IS NOT NULL
    GROUP BY seller_id
),

totals AS (
    SELECT sum(total_revenue) AS grand_total_revenue
    FROM seller_revenue
),

shared AS (
    SELECT
        sr.seller_id,
        s.seller_state,
        sr.total_revenue,
        round(sr.total_revenue / t.grand_total_revenue * 100, 4) AS revenue_share_pct
    FROM seller_revenue sr
    CROSS JOIN totals t
    LEFT JOIN {{ ref('silver_sellers') }} s USING (seller_id)
)

SELECT
    seller_id,
    seller_state,
    total_revenue,
    revenue_share_pct,
    round(power(revenue_share_pct, 2), 4)                                    AS hhi_contribution,
    round(sum(revenue_share_pct) OVER (ORDER BY revenue_share_pct DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 2)              AS cumulative_share_pct,
    row_number() OVER (ORDER BY revenue_share_pct DESC)                      AS revenue_rank,
    -- A single supplier commanding >10% of total spend is a common single-source
    -- procurement heuristic (losing them meaningfully disrupts the business); flagged
    -- independently of the portfolio-wide HHI, which can look healthy in aggregate while
    -- still containing one or two concentrated outliers.
    CASE WHEN revenue_share_pct > 10 THEN 'HIGH' ELSE 'NORMAL' END           AS concentration_flag
FROM shared
ORDER BY revenue_share_pct DESC
