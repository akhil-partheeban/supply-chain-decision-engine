-- Supplier (seller) scorecard: one row per seller with volume, cost, delivery-reliability,
-- and satisfaction metrics rolled into a single composite reliability_score.
--
-- Replaces the old gold_supplier_performance + gold_supplier_risk models, which computed
-- overlapping aggregates (both grouped by seller_id off of nearly-identical joins) and
-- risked drifting apart. One scorecard, one source of truth per seller.

WITH seller_items AS (
    SELECT
        seller_id,
        order_id,
        product_id,
        product_category,
        price,
        freight_value
    FROM {{ ref('silver_order_items') }}
    WHERE seller_id IS NOT NULL
),

-- One row per (seller, order) — order-level facts, de-duplicated so a multi-line order
-- from the same seller doesn't get counted (and its lateness double-weighted) per line.
seller_orders AS (
    SELECT DISTINCT
        si.seller_id,
        o.order_id,
        o.is_delivered,
        o.is_late,
        o.actual_delivery_days
    FROM seller_items si
    JOIN {{ ref('silver_orders') }} o USING (order_id)
),

-- Reviews are also order-grain (one review per order), so de-dupe the same way before
-- averaging — otherwise a 3-item order counts its review score three times.
seller_reviews AS (
    SELECT DISTINCT
        si.seller_id,
        r.order_id,
        r.review_score
    FROM seller_items si
    JOIN {{ source('bronze', 'order_reviews') }} r USING (order_id)
    WHERE r.review_score IS NOT NULL
),

item_agg AS (
    SELECT
        seller_id,
        count(DISTINCT order_id)         AS total_orders,
        count(DISTINCT product_id)       AS unique_products,
        count(DISTINCT product_category) AS unique_categories,
        round(sum(price), 2)             AS total_revenue,
        round(sum(freight_value), 2)     AS total_freight_cost
    FROM seller_items
    GROUP BY seller_id
),

delivery_agg AS (
    SELECT
        seller_id,
        count(*) FILTER (WHERE is_delivered = 1)                             AS delivered_orders,
        round(avg(actual_delivery_days) FILTER (WHERE is_delivered = 1), 2)  AS avg_delivery_days,
        -- Lead-time variability: sample stddev of delivery days per seller. A seller
        -- averaging 8 days but swinging +/-15 is a worse sourcing bet than one steady
        -- at 10 — avg_delivery_days alone can't tell those apart.
        round(stddev_samp(actual_delivery_days) FILTER (WHERE is_delivered = 1), 2)
                                                                              AS delivery_days_stddev,
        round(avg(is_late::DOUBLE), 4)                                       AS late_delivery_rate
    FROM seller_orders
    GROUP BY seller_id
),

review_agg AS (
    SELECT
        seller_id,
        count(*)                        AS review_count,
        round(avg(review_score), 2)     AS avg_review_score,
        round(
            sum(CASE WHEN review_score <= 2 THEN 1 ELSE 0 END)
            / nullif(count(*), 0)::DOUBLE, 4
        )                                AS pct_negative_reviews
    FROM seller_reviews
    GROUP BY seller_id
),

scored AS (
    SELECT
        s.seller_id,
        s.seller_city,
        s.seller_state,
        ia.total_orders,
        ia.unique_products,
        ia.unique_categories,
        ia.total_revenue,
        ia.total_freight_cost,
        round(ia.total_freight_cost / nullif(ia.total_revenue, 0) * 100, 2) AS freight_pct_of_revenue,
        da.avg_delivery_days,
        coalesce(da.delivery_days_stddev, 0)                                 AS delivery_days_stddev,
        coalesce(da.late_delivery_rate, 0)                                   AS late_delivery_rate,
        round(1 - coalesce(da.late_delivery_rate, 0), 4)                     AS on_time_rate,
        ra.review_count,
        coalesce(ra.avg_review_score, 0)                                     AS avg_review_score,
        coalesce(ra.pct_negative_reviews, 0)                                 AS pct_negative_reviews,
        -- Composite reliability score (0-100), weighted:
        --   50% on-time delivery rate      — did the order arrive when promised
        --   30% average review score / 5   — did the customer end up satisfied
        --   20% delivery-day consistency   — 1 - (stddev / 20), floored at 0
        -- The 20-day consistency cap is not arbitrary: across sellers with >=5 delivered
        -- orders, p95 of delivery_days_stddev is ~14 days and p100 is 63 — 20 sits just
        -- above the 95th percentile, so only genuinely erratic sellers hit a 0 consistency
        -- component. See DECISIONS.md for the full derivation and the single-order-seller
        -- caveat (stddev is undefined below 2 orders and is coalesced to 0 = "assumed
        -- consistent," which overstates sellers with too little history to judge).
        round(
            50 * (1 - coalesce(da.late_delivery_rate, 0))
          + 30 * (coalesce(ra.avg_review_score, 0) / 5.0)
          + 20 * greatest(0, 1 - coalesce(da.delivery_days_stddev, 0) / 20.0),
        1) AS reliability_score
    FROM {{ ref('silver_sellers') }} s
    JOIN item_agg ia USING (seller_id)
    LEFT JOIN delivery_agg da USING (seller_id)
    LEFT JOIN review_agg ra USING (seller_id)
)

SELECT
    *,
    -- Cutoffs picked against the observed reliability_score distribution on the full
    -- Olist dataset (median 86.9, p25 79.4, p10 71.3): >=85 LOW / >=70 MEDIUM / else HIGH
    -- yields roughly 57% LOW, 34% MEDIUM, 9% HIGH — HIGH lands close to the dataset's
    -- overall 7.8% late-delivery rate, so "HIGH risk" flags a small, actionable set of
    -- suppliers rather than an arbitrary third of the supply base. See DECISIONS.md.
    CASE
        WHEN reliability_score >= 85 THEN 'LOW'
        WHEN reliability_score >= 70 THEN 'MEDIUM'
        ELSE 'HIGH'
    END AS risk_tier
FROM scored
