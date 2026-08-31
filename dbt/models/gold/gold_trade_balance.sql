-- UN Comtrade trade balance, one row per (reporter, period): total imports, total
-- exports (both FOB, USD), and the resulting balance. Pivots silver's long
-- import/export rows into the wide shape a trade-balance KPI is actually consumed
-- in (nobody wants to compute exports-minus-imports themselves in every query).
SELECT
    reporter_code,
    period,
    max(CASE WHEN flow_type = 'import' THEN fob_value_usd END) AS import_value_usd,
    max(CASE WHEN flow_type = 'export' THEN fob_value_usd END) AS export_value_usd,
    max(CASE WHEN flow_type = 'export' THEN fob_value_usd END)
        - max(CASE WHEN flow_type = 'import' THEN fob_value_usd END) AS trade_balance_usd
FROM {{ ref('silver_comtrade_trade_flows') }}
WHERE commodity_code = 'TOTAL'
GROUP BY reporter_code, period
