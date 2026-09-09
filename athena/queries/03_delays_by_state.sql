SELECT
    customer_state,
    COUNT(*) AS delivered_orders,
    SUM(
        CASE
            WHEN is_delayed = 1 THEN 1
            ELSE 0
        END
    ) AS delayed_orders,
    ROUND(
        100.0 * SUM(
            CASE
                WHEN is_delayed = 1 THEN 1
                ELSE 0
            END
        ) / COUNT(*),
        2
    ) AS delayed_rate_pct,
    ROUND(
        AVG(avg_review_score),
        2
    ) AS avg_review_score
FROM
    olist_analytics_db.orders_analytics
WHERE
    is_delayed IS NOT NULL
GROUP BY
    customer_state
HAVING
    COUNT(*) >= 100
ORDER BY
    delayed_rate_pct DESC;