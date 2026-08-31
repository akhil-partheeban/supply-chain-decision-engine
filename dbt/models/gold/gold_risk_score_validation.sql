-- Out-of-sample backtest: does gold_supplier_scorecard.risk_tier, computed from a
-- seller's PAST orders, actually predict late-delivery outcomes on that seller's
-- FUTURE orders?
--
-- This is deliberately NOT "do HIGH-risk sellers in gold_supplier_scorecard have a
-- higher late_delivery_rate" — that comparison is circular, because
-- late_delivery_rate is literally 50% of the formula that produces risk_tier in the
-- first place (see gold_supplier_scorecard.sql). Checking a score against the same
-- data it was built from always looks good and proves nothing. The only honest test
-- is whether a score built from historical behavior generalizes to behavior it
-- hasn't seen yet — so this model recomputes risk_tier using ONLY orders before a
-- cutoff date, then checks it against actual outcomes strictly after that date.
--
-- Split: orders before 2018-01-01 = training (45,430 orders, Sep 2016-Dec 2017);
-- orders on/after = holdout (54,011 orders, Jan-Aug 2018). Sep-Oct 2018 (20 orders
-- total) is excluded from both — too sparse to be a meaningful tail. The cutoff was
-- picked for a roughly balanced 46/54 split with real volume on both sides, not
-- tuned to produce a particular result.
--
-- Only sellers with >=3 training orders (below that, late_delivery_rate is one or
-- two coin flips, not a rate) AND >=1 holdout order (something to check the score
-- against) are included: 896 of 3,095 sellers (29%). See DECISIONS.md, Phase 5, for
-- exactly what this excludes and why that's an honest limitation, not a convenient
-- one — most excluded sellers are single-order sellers with no track record to
-- score in the first place, not sellers hidden because they'd look bad.
--
-- The training-period scoring logic below is a deliberate literal copy of
-- gold_supplier_scorecard.sql's formula (same weights, same 20-day consistency cap,
-- same 85/70 tier cutoffs) rather than a shared macro — this backtest is only
-- honest if it is provably scoring with the SAME formula actually shipped in
-- production. If gold_supplier_scorecard's formula ever changes, this file must be
-- updated to match or the backtest silently starts validating a model that isn't
-- the one in use.

WITH seller_items AS (
    SELECT seller_id, order_id
    FROM {{ ref('silver_order_items') }}
    WHERE seller_id IS NOT NULL
),

seller_orders AS (
    SELECT DISTINCT
        si.seller_id,
        o.order_id,
        o.purchased_at,
        o.is_delivered,
        o.is_late,
        o.actual_delivery_days
    FROM seller_items si
    JOIN {{ ref('silver_orders') }} o USING (order_id)
),

seller_reviews AS (
    SELECT DISTINCT
        si.seller_id,
        r.order_id,
        r.review_score
    FROM seller_items si
    JOIN {{ source('bronze', 'order_reviews') }} r USING (order_id)
    WHERE r.review_score IS NOT NULL
),

-- ── Training period: score sellers using only what was known before 2018-01-01 ───

training_orders AS (
    SELECT * FROM seller_orders WHERE purchased_at < '2018-01-01'
),

training_reviews AS (
    SELECT sr.*
    FROM seller_reviews sr
    JOIN training_orders t_o USING (seller_id, order_id)
),

training_delivery_agg AS (
    SELECT
        seller_id,
        count(*)                                                                    AS training_orders,
        round(stddev_samp(actual_delivery_days) FILTER (WHERE is_delivered = 1), 2)  AS delivery_days_stddev,
        round(avg(is_late::DOUBLE), 4)                                               AS late_delivery_rate
    FROM training_orders
    GROUP BY seller_id
),

training_review_agg AS (
    SELECT seller_id, round(avg(review_score), 2) AS avg_review_score
    FROM training_reviews
    GROUP BY seller_id
),

training_scored AS (
    SELECT
        da.seller_id,
        da.training_orders,
        coalesce(da.late_delivery_rate, 0) AS training_late_delivery_rate,
        round(
            50 * (1 - coalesce(da.late_delivery_rate, 0))
          + 30 * (coalesce(ra.avg_review_score, 0) / 5.0)
          + 20 * greatest(0, 1 - coalesce(da.delivery_days_stddev, 0) / 20.0),
        1) AS training_reliability_score
    FROM training_delivery_agg da
    LEFT JOIN training_review_agg ra USING (seller_id)
),

training_tiered AS (
    SELECT
        *,
        CASE
            WHEN training_reliability_score >= 85 THEN 'LOW'
            WHEN training_reliability_score >= 70 THEN 'MEDIUM'
            ELSE 'HIGH'
        END AS training_risk_tier
    FROM training_scored
),

-- ── Holdout period: what actually happened next, per seller ─────────────────────

holdout_orders AS (
    SELECT * FROM seller_orders WHERE purchased_at >= '2018-01-01'
),

holdout_outcomes AS (
    SELECT
        seller_id,
        count(*)           AS holdout_orders,
        sum(is_late)        AS holdout_late_orders
    FROM holdout_orders
    GROUP BY seller_id
),

backtest AS (
    SELECT
        t.seller_id,
        t.training_orders,
        t.training_reliability_score,
        t.training_risk_tier,
        h.holdout_orders,
        h.holdout_late_orders
    FROM training_tiered t
    JOIN holdout_outcomes h USING (seller_id)
    WHERE t.training_orders >= 3
      AND h.holdout_orders >= 1
),

tier_agg AS (
    SELECT
        training_risk_tier,
        count(DISTINCT seller_id) AS sellers,
        sum(holdout_orders)       AS holdout_orders,
        sum(holdout_late_orders)  AS holdout_late_orders
    FROM backtest
    GROUP BY training_risk_tier
),

totals AS (
    SELECT
        sum(sellers)              AS total_sellers,
        sum(holdout_orders)       AS total_holdout_orders,
        sum(holdout_late_orders)  AS total_holdout_late_orders
    FROM tier_agg
)

SELECT
    ta.training_risk_tier,
    ta.sellers,
    ta.holdout_orders,
    ta.holdout_late_orders,
    round(ta.holdout_late_orders / nullif(ta.holdout_orders, 0)::DOUBLE, 4)         AS holdout_late_rate,
    round(t.total_holdout_late_orders / nullif(t.total_holdout_orders, 0)::DOUBLE, 4) AS baseline_late_rate,
    round(ta.sellers / t.total_sellers::DOUBLE, 4)                                   AS pct_of_sellers,
    round(ta.holdout_late_orders / nullif(t.total_holdout_late_orders, 0)::DOUBLE, 4) AS pct_of_holdout_late_orders,
    -- Two different, both-honest "lift" numbers — they diverge, and the divergence
    -- itself is the finding (see DECISIONS.md, Phase 5, for the full explanation):
    --
    -- rate_lift_vs_baseline: this tier's per-order holdout late rate, divided by the
    -- overall baseline late rate. Answers "is an order shipped through a
    -- HIGH-flagged seller more likely to be late than a random order?" — the
    -- correct lens for per-order/per-transaction risk, and unaffected by how much
    -- future business a seller happens to do.
    round(
        (ta.holdout_late_orders / nullif(ta.holdout_orders, 0)::DOUBLE)
        / nullif(t.total_holdout_late_orders / nullif(t.total_holdout_orders, 0)::DOUBLE, 0)
    , 2) AS rate_lift_vs_baseline,
    -- seller_selection_lift: this tier's share of total future late-order VOLUME,
    -- divided by this tier's share of sellers. Answers "if I could only investigate
    -- N sellers, does flagging this tier surface more raw failure count than
    -- randomly picking N sellers?" — confounded by order volume: a tier full of
    -- low-volume sellers can have a much worse late RATE while still contributing a
    -- smaller share of the company's total late-order COUNT than its seller-count
    -- share would suggest, simply because it ships fewer orders overall.
    round(
        (ta.holdout_late_orders / nullif(t.total_holdout_late_orders, 0)::DOUBLE)
        / nullif(ta.sellers / t.total_sellers::DOUBLE, 0)
    , 2) AS seller_selection_lift
FROM tier_agg ta
CROSS JOIN totals t
ORDER BY CASE ta.training_risk_tier WHEN 'HIGH' THEN 1 WHEN 'MEDIUM' THEN 2 ELSE 3 END
