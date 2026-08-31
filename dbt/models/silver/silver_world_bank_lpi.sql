-- Cleaned World Bank Logistics Performance Index records. Grain: one row per
-- (country, indicator, year) — kept in this "long" / tidy shape deliberately (not
-- pivoted wide here) because that's the natural cleaned grain of the source data;
-- gold_country_logistics_scorecard does the pivot into one-row-per-country for
-- consumption, which is where a pivot belongs per this project's layering
-- convention (silver = natural grain, gold = shaped for consumption).
SELECT
    country_code,
    country_name,
    indicator_code,
    indicator_name,
    CAST(year AS INTEGER) AS year,
    CAST(value AS DOUBLE) AS value,
    _loaded_at
FROM {{ source('bronze', 'world_bank_lpi') }}
WHERE value IS NOT NULL
