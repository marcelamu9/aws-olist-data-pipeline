
WITH quality_metrics AS (
    SELECT
        COUNT(*) AS total_orders,

        COUNT(DISTINCT order_id) AS unique_orders,

        COUNT_IF(order_id IS NULL) AS null_order_ids,

        COUNT_IF(
            is_delayed IS NOT NULL
            AND is_delayed NOT IN (0, 1)
        ) AS invalid_delay_labels

    FROM olist_analytics_db.orders_analytics
)

SELECT
    total_orders,
    unique_orders,
    total_orders - unique_orders AS duplicate_orders,
    null_order_ids,
    invalid_delay_labels,

    CASE
        WHEN total_orders > 0
            AND total_orders = unique_orders
            AND null_order_ids = 0
            AND invalid_delay_labels = 0
        THEN 'PASS'
        ELSE 'FAIL'
    END AS quality_status

FROM quality_metrics;