SELECT
    product_category_name_english AS product_category,
    COUNT(DISTINCT order_id) AS delivered_orders,
    COUNT(
        DISTINCT CASE
            WHEN is_delayed = 1 THEN order_id
        END
    ) AS delayed_orders,
    ROUND(
        100.0 * COUNT(
            DISTINCT CASE
                WHEN is_delayed = 1 THEN order_id
            END
        ) / COUNT(DISTINCT order_id),
        2
    ) AS delayed_rate_pct,
    ROUND(
        AVG(avg_review_score),
        2
    ) AS avg_review_score,
    ROUND(
        AVG(actual_delivery_days),
        2
    ) AS avg_delivery_days
FROM
    olist_analytics_db.category_analytics
WHERE
    is_delayed IS NOT NULL
    AND product_category_name_english IS NOT NULL
GROUP BY
    product_category_name_english
HAVING
    COUNT(DISTINCT order_id) >= 100
ORDER BY
    delayed_rate_pct DESC;