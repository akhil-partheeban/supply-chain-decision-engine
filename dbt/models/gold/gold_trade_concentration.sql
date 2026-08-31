-- Trade-partner concentration, per (reporter, commodity, flow, period): which
-- source countries dominate this product's supply, and how concentrated is that
-- supply base? Same HHI methodology as gold_concentration_risk (Phase 1) — sum of
-- squared partner-share-of-value percentages, 0-10,000 scale, DOJ/FTC merger-
-- guideline interpretation bands — reused deliberately for methodological
-- consistency across the project: this project uses one concentration index
-- throughout (supplier revenue in Phase 1, geography in Phase 1, trade partners
-- here), not a different ad-hoc formula per use case.
--
-- Grain: one row per (reporter, commodity, flow, period, partner) — group-level
-- stats (group_hhi, its interpretation) are denormalized onto every partner row in
-- the group, which is what makes both "top partners for this product" and "HHI
-- trend for this product over time" answerable from one table without a second join.

WITH partner_totals AS (
    SELECT
        reporter_code,
        period,
        flow_type,
        commodity_code,
        partner_code,
        partner_name,
        sum(fob_value_usd) AS fob_value_usd
    FROM {{ ref('silver_comtrade_partner_flows') }}
    GROUP BY 1, 2, 3, 4, 5, 6
),

group_totals AS (
    SELECT reporter_code, period, flow_type, commodity_code, sum(fob_value_usd) AS group_total_usd
    FROM partner_totals
    GROUP BY 1, 2, 3, 4
),

shared AS (
    SELECT
        pt.*,
        round(pt.fob_value_usd / nullif(gt.group_total_usd, 0) * 100, 4) AS partner_share_pct
    FROM partner_totals pt
    JOIN group_totals gt USING (reporter_code, period, flow_type, commodity_code)
)

SELECT
    reporter_code,
    period,
    flow_type,
    commodity_code,
    partner_code,
    partner_name,
    fob_value_usd,
    partner_share_pct,
    round(power(partner_share_pct, 2), 4) AS hhi_contribution,
    row_number() OVER (
        PARTITION BY reporter_code, period, flow_type, commodity_code
        ORDER BY partner_share_pct DESC
    ) AS partner_rank,
    round(sum(partner_share_pct) OVER (
        PARTITION BY reporter_code, period, flow_type, commodity_code
        ORDER BY partner_share_pct DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ), 2) AS cumulative_share_pct,
    round(sum(power(partner_share_pct, 2)) OVER (
        PARTITION BY reporter_code, period, flow_type, commodity_code
    ), 2) AS group_hhi,
    CASE
        WHEN sum(power(partner_share_pct, 2)) OVER (PARTITION BY reporter_code, period, flow_type, commodity_code) >= 2500
            THEN 'HIGHLY_CONCENTRATED'
        WHEN sum(power(partner_share_pct, 2)) OVER (PARTITION BY reporter_code, period, flow_type, commodity_code) >= 1500
            THEN 'MODERATELY_CONCENTRATED'
        ELSE 'UNCONCENTRATED'
    END AS group_hhi_interpretation
FROM shared
ORDER BY reporter_code, commodity_code, period, flow_type, partner_rank
