SELECT
    CASE
        WHEN is_delayed = 1 THEN 'Delayed'
        WHEN is_delayed = 0 THEN 'On time'
    END AS delivery_status,
    COUNT(*) AS orders,
    ROUND(
        AVG(avg_review_score),
        2
    ) AS avg_review_score
FROM
    olist_analytics_db.orders_analytics
WHERE
    is_delayed IS NOT NULL
    AND avg_review_score IS NOT NULL
GROUP BY
    CASE
        WHEN is_delayed = 1 THEN 'Delayed'
        WHEN is_delayed = 0 THEN 'On time'
    END
ORDER BY
    delivery_status;