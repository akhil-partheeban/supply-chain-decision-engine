-- Seller dimension, cleaned. Deliberately NOT aggregated here — grain stays one row per
-- seller_id so gold models decide how to roll up order/item facts against it. (An earlier
-- version of this model pre-aggregated order counts and late-delivery rates directly in
-- silver, which meant three different models were independently re-deriving the same
-- "is this order late" logic. That logic now lives once, in silver_orders.is_late.)
SELECT
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state,
    _source_file,
    _loaded_at
FROM {{ source('bronze', 'sellers') }}
WHERE seller_id IS NOT NULL
