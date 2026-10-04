-- Blends gold_supplier_scorecard.reliability_score with
-- gold_supplier_emissions.emissions_intensity into one score per seller, so
-- "low risk" and "low emissions" can be weighed together rather than read off
-- two separate tables. Higher risk_emissions_score = better (both low risk and
-- low emissions) — same direction as reliability_score, so this reads naturally
-- next to it.
--
-- Both inputs are converted to a percentile via percent_rank(), not min-max
-- scaling, specifically to resist outliers: gold_supplier_emissions has sellers
-- with very large or very small emissions_intensity (a handful of extreme values
-- would compress every other seller's min-max-scaled score toward one end of the
-- range); a percentile rank only cares about relative order, so one extreme
-- outlier can't distort everyone else's score. See DECISIONS.md, Phase 8.
--
-- The 60 sellers with no resolvable product category (see gold_supplier_emissions)
-- have a NULL emissions_intensity and are deliberately excluded from the
-- percent_rank() window itself (not included with some default value) — their
-- risk_emissions_score is NULL here too, honestly reflecting "no basis to score
-- this," not a fabricated best- or worst-case score.

WITH emissions_ranked AS (
    SELECT
        seller_id,
        emissions_intensity,
        -- Ascending: higher percentile = higher (worse) intensity.
        percent_rank() OVER (ORDER BY emissions_intensity) AS intensity_percentile
    FROM {{ ref('gold_supplier_emissions') }}
    WHERE emissions_intensity IS NOT NULL
),

risk_ranked AS (
    SELECT
        seller_id,
        reliability_score,
        -- Ascending: higher percentile = higher (better) reliability.
        percent_rank() OVER (ORDER BY reliability_score) AS reliability_percentile
    FROM {{ ref('gold_supplier_scorecard') }}
)

SELECT
    r.seller_id,
    r.reliability_score,
    round(r.reliability_percentile, 4)        AS reliability_percentile,
    e.emissions_intensity,
    round(e.intensity_percentile, 4)          AS emissions_intensity_percentile,
    -- Inverted so higher = better (lower intensity), matching reliability_percentile's
    -- own direction — this is the figure actually used in the blend below.
    round(1 - e.intensity_percentile, 4)      AS emissions_percentile_good,
    CASE
        WHEN e.intensity_percentile IS NOT NULL THEN
            round(
                {{ var('emissions_score_risk_weight') }} * r.reliability_percentile
              + (1 - {{ var('emissions_score_risk_weight') }}) * (1 - e.intensity_percentile),
            4)
    END AS risk_emissions_score
FROM risk_ranked r
LEFT JOIN emissions_ranked e USING (seller_id)
