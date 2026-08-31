-- Cleaned UN Comtrade WORLD-AGGREGATE trade flow totals. Grain: one row per
-- (reporter, period, flow).
--
-- The raw bronze table carries one row per (reporter, period, flow, partner,
-- mode-of-transport) — since Phase 6 added partner-level pulls (see
-- silver_comtrade_partner_flows.sql), bronze now holds a MIX of world-aggregate
-- rows (partnerCode = 0, from Phase 4's one-off CLI pull) and real per-partner
-- rows (partnerCode = an actual country, from the Phase 6 weekly pipeline). This
-- model explicitly keeps only partnerCode = 0 — before Phase 6, that filter was
-- implicit (every row in bronze happened to be a world-aggregate pull, so it
-- didn't need stating), and leaving it implicit would now silently blend two
-- different grains together the moment both kinds of pull share one bronze table.
--
-- Within the world-aggregate rows, motCode = 0 ("TOTAL MOT") is Comtrade's own
-- precomputed sum across all modes of transport (air/sea/rail/road/etc — the other
-- motCode values); summing every motCode row would double-count, since 0 already
-- IS that sum, not an additional component alongside them.
SELECT
    CAST(reporterCode AS BIGINT)         AS reporter_code,
    period,
    flowCode                             AS flow_code,
    CASE flowCode WHEN 'M' THEN 'import' WHEN 'X' THEN 'export' ELSE flowCode END AS flow_type,
    cmdCode                              AS commodity_code,
    fobvalue                             AS fob_value_usd,
    cifvalue                             AS cif_value_usd,
    _loaded_at
FROM {{ source('bronze', 'comtrade_trade_flows') }}
WHERE motCode = 0
  AND partnerCode = 0
