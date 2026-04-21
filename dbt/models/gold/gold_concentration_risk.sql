WITH state_totals AS (
    SELECT
        seller_state,
        COUNT(DISTINCT seller_id) AS total_sellers,
        SUM(total_orders)         AS total_orders
    FROM {{ ref('silver_sellers') }}
    WHERE total_orders > 0
    GROUP BY seller_state
),

grand_total AS (
    SELECT SUM(total_orders) AS grand_total_orders
    FROM {{ ref('silver_sellers') }}
    WHERE total_orders > 0
)

SELECT
    st.seller_state,
    st.total_sellers,
    st.total_orders,
    ROUND(st.total_orders / gt.grand_total_orders, 4) AS pct_of_total_orders,
    CASE
        WHEN st.total_orders / gt.grand_total_orders > 0.2 THEN 'HIGH'
        ELSE 'NORMAL'
    END AS concentration_flag
FROM state_totals st
CROSS JOIN grand_total gt
ORDER BY st.total_orders DESC
