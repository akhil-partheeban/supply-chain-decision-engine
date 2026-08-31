-- Geographic concentration of sourcing spend, by seller state. Distinct from
-- gold_concentration_risk (which measures dependency on individual sellers): a supply
-- base can be spread across a thousand sellers and still be geographically concentrated
-- if 900 of them sit in one state — a single regional disruption (customs, weather,
-- logistics strike) then hits most of the business at once.
WITH state_totals AS (
    SELECT
        s.seller_state,
        count(DISTINCT s.seller_id)       AS total_sellers,
        round(sum(oi.price), 2)           AS total_revenue
    FROM {{ ref('silver_sellers') }} s
    JOIN {{ ref('silver_order_items') }} oi USING (seller_id)
    GROUP BY s.seller_state
),

grand_total AS (
    SELECT sum(total_revenue) AS grand_total_revenue
    FROM state_totals
)

SELECT
    st.seller_state,
    st.total_sellers,
    st.total_revenue,
    round(st.total_revenue / gt.grand_total_revenue * 100, 2) AS pct_of_total_revenue,
    -- 20% threshold: with 27 Brazilian states in the seller base, an even split would put
    -- each state at ~3.7% — any single state clearing 20% (5-6x the "even" share) is a
    -- real geographic dependency, not noise. In this dataset, São Paulo alone runs ~64%.
    CASE WHEN st.total_revenue / gt.grand_total_revenue > 0.2 THEN 'HIGH' ELSE 'NORMAL' END
                                                                AS concentration_flag
FROM state_totals st
CROSS JOIN grand_total gt
ORDER BY st.total_revenue DESC
