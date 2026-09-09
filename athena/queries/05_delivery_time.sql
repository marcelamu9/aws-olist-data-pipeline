SELECT
    COUNT(*) AS delivered_orders,
    ROUND(
        AVG(estimated_delivery_days),
        2
    ) AS avg_estimated_delivery_days,
    ROUND(
        AVG(actual_delivery_days),
        2
    ) AS avg_actual_delivery_days,
    ROUND(AVG(delay_days), 2) AS avg_delay_days
FROM
    olist_analytics_db.orders_analytics
WHERE
    actual_delivery_days IS NOT NULL;