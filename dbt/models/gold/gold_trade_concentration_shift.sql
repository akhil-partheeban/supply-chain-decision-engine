-- Period-over-period shift in trade-partner concentration: for each
-- (reporter, commodity, flow), how did each partner's share of supply — and the
-- group's overall HHI — change from the previous available period to this one?
--
-- "Previous available period" means whatever period this specific (reporter,
-- commodity, flow, partner) last had data for, via LAG() ordered by period — NOT
-- necessarily "N-1 calendar years ago." Comtrade doesn't guarantee every partner
-- reports every year, so this is the correct definition of "prior" for a data
-- source with gaps, at the cost of a subtlety worth being explicit about: two
-- partners' "prior period" for the same product/reporter/flow could technically be
-- different years if one has a reporting gap the other doesn't. In this project's
-- actual data (2 periods pulled so far, 2024 and 2025, both dense), every partner's
-- prior period is simply "the other one" — but the LAG-based definition is what
-- makes this model still correct once more periods and any real reporting gaps
-- accumulate.
--
-- Rows with no prior period (every partner's very first appearance in the data) are
-- excluded — there is nothing to compute a shift against yet. This is why a
-- freshly-bootstrapped database with only one period pulled will find this model
-- returns zero rows: shift is a second-run-onward concept by definition, not a bug.

WITH base AS (
    SELECT
        reporter_code, commodity_code, flow_type, period, partner_code, partner_name,
        partner_share_pct, group_hhi, group_hhi_interpretation
    FROM {{ ref('gold_trade_concentration') }}
),

with_prior AS (
    SELECT
        *,
        lag(period) OVER (
            PARTITION BY reporter_code, commodity_code, flow_type, partner_code ORDER BY period
        ) AS prior_period,
        lag(partner_share_pct) OVER (
            PARTITION BY reporter_code, commodity_code, flow_type, partner_code ORDER BY period
        ) AS prior_partner_share_pct,
        lag(group_hhi) OVER (
            PARTITION BY reporter_code, commodity_code, flow_type, partner_code ORDER BY period
        ) AS prior_group_hhi
    FROM base
)

SELECT
    reporter_code,
    commodity_code,
    flow_type,
    period,
    prior_period,
    partner_code,
    partner_name,
    partner_share_pct,
    prior_partner_share_pct,
    round(partner_share_pct - prior_partner_share_pct, 2) AS partner_share_pct_change,
    group_hhi,
    prior_group_hhi,
    round(group_hhi - prior_group_hhi, 2) AS group_hhi_change,
    group_hhi_interpretation
FROM with_prior
WHERE prior_period IS NOT NULL
ORDER BY reporter_code, commodity_code, flow_type, period, partner_share_pct_change DESC
