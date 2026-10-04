-- For each high-emission supplier, suggest the single best same-category
-- alternative: lowest intensity, no worse on risk (within a tolerance), and with
-- enough order volume that the suggestion is a plausible sourcing switch rather
-- than a one-order fluke. Grain: one row per original supplier that both (a)
-- qualifies as "high-emission" and (b) has at least one qualifying alternative —
-- a flagged supplier with no plausible alternative simply produces no row, rather
-- than a row with a NULL suggestion.
--
-- "High-emission" means at or above var('emissions_swap_threshold_percentile')
-- (default 0.75 = top quartile) of emissions_intensity WITHIN that supplier's own
-- primary_category — a category-relative bar, not a global one, since what counts
-- as "high" for perfumery and for office furniture are very different absolute
-- numbers.

WITH sellers_full AS (
    -- One row per seller with everything both sides of the comparison need. Pulled
    -- fresh from gold_supplier_scorecard (reliability_score, total_orders) rather
    -- than duplicating those figures into gold_supplier_emissions — one source of
    -- truth per fact, same convention as the rest of this project.
    SELECT
        e.seller_id,
        e.primary_category,
        e.emissions_intensity,
        e.mapped_spend_usd,
        s.reliability_score,
        s.total_orders
    FROM {{ ref('gold_supplier_emissions') }} e
    JOIN {{ ref('gold_supplier_scorecard') }} s USING (seller_id)
    WHERE e.primary_category IS NOT NULL
      AND e.emissions_intensity IS NOT NULL
),

ranked AS (
    SELECT
        *,
        percent_rank() OVER (
            PARTITION BY primary_category ORDER BY emissions_intensity
        ) AS category_intensity_percentile
    FROM sellers_full
),

originals AS (
    SELECT * FROM ranked
    WHERE category_intensity_percentile >= {{ var('emissions_swap_threshold_percentile') }}
),

-- Non-equi self-join: every (original, alternative) pair in the same category
-- that clears all three swap conditions at once (genuinely lower emissions, risk
-- no worse than tolerance, enough volume to be plausible). This can — and often
-- does — produce several qualifying alternatives per original; the window
-- function below picks the single best one.
candidates AS (
    SELECT
        o.seller_id          AS original_seller_id,
        o.primary_category,
        o.emissions_intensity AS original_intensity,
        o.reliability_score   AS original_reliability_score,
        o.mapped_spend_usd    AS original_mapped_spend_usd,
        a.seller_id           AS alternative_seller_id,
        a.emissions_intensity AS alternative_intensity,
        a.reliability_score   AS alternative_reliability_score,
        a.total_orders        AS alternative_total_orders,
        -- Best alternative first: lowest intensity; ties broken by higher volume
        -- (a more-established seller is a safer bet), then seller_id for a fully
        -- deterministic result on an exact tie.
        row_number() OVER (
            PARTITION BY o.seller_id
            ORDER BY a.emissions_intensity ASC, a.total_orders DESC, a.seller_id ASC
        ) AS alt_rank
    FROM originals o
    JOIN sellers_full a
        ON a.primary_category = o.primary_category
       AND a.seller_id != o.seller_id
       AND a.emissions_intensity < o.emissions_intensity
       AND a.reliability_score >= o.reliability_score - {{ var('emissions_swap_risk_tolerance') }}
       AND a.total_orders >= {{ var('emissions_swap_min_order_volume') }}
)

SELECT
    original_seller_id,
    primary_category,
    original_intensity,
    original_reliability_score,
    alternative_seller_id,
    alternative_intensity,
    alternative_reliability_score,
    alternative_total_orders,
    round((original_intensity - alternative_intensity) / original_intensity * 100, 2) AS estimated_pct_emissions_reduction,
    -- Hypothetical: if the original's own mapped spend were redirected to the
    -- alternative's intensity instead, this many fewer kg CO2e — a what-if, not a
    -- measured reduction (the swap hasn't happened).
    round(original_mapped_spend_usd * (original_intensity - alternative_intensity), 4) AS estimated_kg_co2e_reduction
FROM candidates
WHERE alt_rank = 1
ORDER BY estimated_kg_co2e_reduction DESC
