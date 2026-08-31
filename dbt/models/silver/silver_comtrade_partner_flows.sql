-- Cleaned UN Comtrade PARTNER-LEVEL trade flow totals. Grain: one row per
-- (reporter, period, flow, commodity, partner country). This is the fact table
-- gold_trade_concentration needs — "which countries dominate as a source for this
-- product" is a question about the partner breakdown, not the world-aggregate total
-- silver_comtrade_trade_flows carries.
--
-- Same motCode = 0 ("TOTAL MOT") logic as silver_comtrade_trade_flows — keep only
-- each partner's total across all transport modes, not every mode as a separate row.
-- partnerCode != 0 excludes the world-aggregate rows (those belong to the other
-- model); partnerCode 0 is Comtrade's own "World" pseudo-partner, not a country.
--
-- partner_name comes from the comtrade_partner_areas seed (dbt/seeds/), pulled
-- directly from Comtrade's own reference endpoint
-- (https://comtradeapi.un.org/files/v1/app/reference/partnerAreas.json) rather than
-- hand-typed — see DECISIONS.md, Phase 6, for why that distinction mattered here.
--
-- The seed's own `is_group` flag does NOT mark residual/regional pseudo-partners
-- (e.g. "Other Asia, nes", "Africa CAMEU region, nes", "Bunkers", "Free Zones") as
-- groups — verified directly against the reference data, not assumed; every one of
-- them has is_group = false. Comtrade's ", nes" ("not elsewhere specified") suffix
-- is the actual, verified marker for "this is a residual/regional bucket, not a
-- single real country" — checked against all 310 reference rows to confirm the
-- pattern catches exactly the aggregate entries and no real country (a naive
-- '%nes%' substring match would incorrectly exclude Indonesia, the Philippines,
-- French Polynesia, FS Micronesia, and Saint Vincent and the Grenadines, all of
-- which contain "nes" as a substring of their real name — hence the exact ', nes'
-- suffix, not a bare substring match). "Bunkers"/"Free Zones"/"Special Categories"
-- are Comtrade's other non-country codes, listed explicitly since they don't share
-- the ", nes" suffix pattern.
SELECT
    CAST(f.reporterCode AS BIGINT)        AS reporter_code,
    f.period,
    f.flowCode                            AS flow_code,
    CASE f.flowCode WHEN 'M' THEN 'import' WHEN 'X' THEN 'export' ELSE f.flowCode END AS flow_type,
    f.cmdCode                             AS commodity_code,
    CAST(f.partnerCode AS BIGINT)         AS partner_code,
    coalesce(p.partner_desc, 'Unknown (' || f.partnerCode || ')') AS partner_name,
    f.fobvalue                            AS fob_value_usd,
    f.cifvalue                            AS cif_value_usd,
    f._loaded_at,
    f._run_id
FROM {{ source('bronze', 'comtrade_trade_flows') }} f
LEFT JOIN {{ ref('comtrade_partner_areas') }} p
    ON CAST(f.partnerCode AS BIGINT) = p.partner_code
WHERE f.motCode = 0
  AND f.partnerCode != 0
  AND coalesce(p.partner_desc NOT LIKE '%, nes', true)
  AND coalesce(p.partner_desc NOT IN ('Bunkers', 'Free Zones', 'Special Categories'), true)
