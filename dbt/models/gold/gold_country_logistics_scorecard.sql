-- World Bank LPI scorecard, one row per country, pivoted wide from silver's tidy
-- long format. Uses each indicator's own most-recent available year rather than a
-- single shared year across all 7 indicators — the LPI survey doesn't necessarily
-- publish every sub-indicator for every country in the same release cycle, so
-- requiring one common year would drop data that's genuinely the latest available
-- for that specific indicator.
--
-- lpi_rank_in_dataset ranks only the countries actually loaded into this database
-- (see ingestion/world_bank_lpi.py --countries) — it is NOT a global World Bank
-- ranking. Naming it "in_dataset" instead of just "rank" is deliberate, so nobody
-- downstream mistakes a 5-country demo rank for Brazil's real global LPI rank.

WITH latest_year AS (
    SELECT country_code, indicator_code, max(year) AS year
    FROM {{ ref('silver_world_bank_lpi') }}
    GROUP BY country_code, indicator_code
),

latest_values AS (
    SELECT s.country_code, s.country_name, s.indicator_code, s.year, s.value
    FROM latest_year ly
    JOIN {{ ref('silver_world_bank_lpi') }} s
        ON ly.country_code = s.country_code
       AND ly.indicator_code = s.indicator_code
       AND ly.year = s.year
),

pivoted AS (
    SELECT
        country_code,
        max(country_name) AS country_name,
        max(CASE WHEN indicator_code = 'LP.LPI.OVRL.XQ' THEN value END) AS lpi_overall,
        max(CASE WHEN indicator_code = 'LP.LPI.CUST.XQ' THEN value END) AS lpi_customs,
        max(CASE WHEN indicator_code = 'LP.LPI.INFR.XQ' THEN value END) AS lpi_infrastructure,
        max(CASE WHEN indicator_code = 'LP.LPI.ITRN.XQ' THEN value END) AS lpi_international_shipments,
        max(CASE WHEN indicator_code = 'LP.LPI.LOGS.XQ' THEN value END) AS lpi_logistics_competence,
        max(CASE WHEN indicator_code = 'LP.LPI.TRAC.XQ' THEN value END) AS lpi_tracking_tracing,
        max(CASE WHEN indicator_code = 'LP.LPI.TIME.XQ' THEN value END) AS lpi_timeliness,
        max(year) AS most_recent_indicator_year
    FROM latest_values
    GROUP BY country_code
)

SELECT
    *,
    rank() OVER (ORDER BY lpi_overall DESC) AS lpi_rank_in_dataset
FROM pivoted
WHERE lpi_overall IS NOT NULL
