-- Single-row macro backdrop for Brazil — the country the entire Olist-based supply
-- chain analysis in this project is actually about. Pairs the World Bank's
-- assessment of Brazil's national logistics infrastructure with Brazil's own trade
-- balance, so a decision-maker reading the Olist-derived supplier scorecards
-- (gold_supplier_scorecard) has the macro context alongside the micro,
-- transaction-level risk signals: e.g. "suppliers are mostly reliable AND the
-- country's customs/infrastructure LPI sub-scores are weak" is a very different
-- risk story than either fact alone.
--
-- FULL OUTER JOIN (not a plain join) deliberately: if only one of the two source
-- pulls has been run (see ingestion/world_bank_lpi.py / ingestion/comtrade.py),
-- this still returns one row with NULLs for whichever side is missing, rather than
-- silently returning zero rows and breaking any dashboard/API query expecting a
-- single-row result.

WITH brazil_lpi AS (
    SELECT *
    FROM {{ ref('gold_country_logistics_scorecard') }}
    WHERE country_code = 'BRA'
),

brazil_trade AS (
    SELECT *
    FROM {{ ref('gold_trade_balance') }}
    WHERE reporter_code = 76  -- ISO numeric code for Brazil
    ORDER BY period DESC
    LIMIT 1
)

SELECT
    'Brazil'                              AS country_name,
    bl.lpi_overall,
    bl.lpi_customs,
    bl.lpi_infrastructure,
    bl.lpi_international_shipments,
    bl.lpi_logistics_competence,
    bl.lpi_tracking_tracing,
    bl.lpi_timeliness,
    bl.most_recent_indicator_year         AS lpi_year,
    bl.lpi_rank_in_dataset,
    bt.period                             AS trade_period,
    bt.import_value_usd,
    bt.export_value_usd,
    bt.trade_balance_usd
FROM brazil_lpi bl
FULL OUTER JOIN brazil_trade bt ON true
