-- Fails if any seller with at least one order item is missing from
-- gold_supplier_emissions. A seller can legitimately have a NULL
-- emissions_intensity there (no resolvable category) but must still appear as a
-- row — this is the check that the model never silently drops a seller.
SELECT DISTINCT oi.seller_id
FROM {{ ref('silver_order_items') }} oi
LEFT JOIN {{ ref('gold_supplier_emissions') }} e
    ON oi.seller_id = e.seller_id
WHERE oi.seller_id IS NOT NULL
  AND e.seller_id IS NULL
