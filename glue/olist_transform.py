# inicializar Glue/Spark;
# leer orders desde olist_raw_db;
# convertir fechas string → timestamp;
# revisar nulos;
# mostrar esquema y unas filas.

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import Window
from pyspark.sql.functions import (
    avg,
    col,
    count,
    countDistinct,
    datediff,
    max as spark_max,
    row_number,
    sum as spark_sum,
    to_timestamp,
    when,
    year,
    month,
    dayofweek,
)

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(args["JOB_NAME"], args)


orders_dynamic_frame = glue_context.create_dynamic_frame.from_catalog(
    database="olist_raw_db",
    table_name="orders",
)

orders_df = orders_dynamic_frame.toDF()


DATE_COLUMNS = [
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
]

for column_name in DATE_COLUMNS:
    orders_df = orders_df.withColumn(
        column_name,
        to_timestamp(
            col(column_name),
            "yyyy-MM-dd HH:mm:ss",
        ),
    )


orders_df = (
    orders_df
    .withColumn(
        "estimated_delivery_days",
        datediff(
            col("order_estimated_delivery_date"),
            col("order_purchase_timestamp"),
        ),
    )
    .withColumn(
        "actual_delivery_days",
        when(
            col("order_delivered_customer_date").isNotNull(),
            datediff(
                col("order_delivered_customer_date"),
                col("order_purchase_timestamp"),
            ),
        ),
    )
    .withColumn(
        "delay_days",
        when(
            col("order_delivered_customer_date").isNotNull(),
            datediff(
                col("order_delivered_customer_date"),
                col("order_estimated_delivery_date"),
            ),
        ),
    )
    .withColumn(
        "is_delayed",
        when(
            col("order_delivered_customer_date").isNull(),
            None,
        )
        .when(
            col("order_delivered_customer_date")
            > col("order_estimated_delivery_date"),
            1,
        )
        .otherwise(0),
    )
)


customers_df = (
    glue_context.create_dynamic_frame.from_catalog(
        database="olist_raw_db",
        table_name="customers",
    )
    .toDF()
)

order_items_df = (
    glue_context.create_dynamic_frame.from_catalog(
        database="olist_raw_db",
        table_name="order_items",
    )
    .toDF()
)


products_df = (
    glue_context.create_dynamic_frame.from_catalog(
        database="olist_raw_db",
        table_name="products",
    )
    .toDF()
)

category_translation_df = (
    glue_context.create_dynamic_frame.from_catalog(
        database="olist_raw_db",
        table_name="category_translation",
    )
    .toDF()
)

reviews_df = (
    glue_context.create_dynamic_frame.from_catalog(
        database="olist_raw_db",
        table_name="order_reviews",
    )
    .toDF()
)


customers_df = customers_df.select(
    "customer_id",
    "customer_unique_id",
    "customer_city",
    "customer_state",
)

products_df = products_df.select(
    "product_id",
    "product_category_name",
    "product_weight_g",
    "product_length_cm",
    "product_height_cm",
    "product_width_cm",
)

order_items_agg_df = (
    order_items_df
    .groupBy("order_id")
    .agg(
        count("*").alias("item_count"),
        countDistinct("product_id").alias("product_count"),
        countDistinct("seller_id").alias("seller_count"),
        spark_sum("price").alias("total_price"),
        spark_sum("freight_value").alias("total_freight"),
    )
    .withColumn(
        "order_value",
        col("total_price") + col("total_freight"),
    )
)

orders_enriched_df = (
    orders_df
    .join(
        customers_df,
        on="customer_id",
        how="left",
    )
    .join(
        order_items_agg_df,
        on="order_id",
        how="left",
    )
)


products_df = products_df.withColumn(
    "product_volume_cm3",
    col("product_length_cm")
    * col("product_height_cm")
    * col("product_width_cm"),
)

products_enriched_df = (
    products_df
    .join(
        category_translation_df,
        on="product_category_name",
        how="left",
    )
)

items_products_df = (
    order_items_df
    .join(
        products_enriched_df,
        on="product_id",
        how="left",
    )
)

product_metrics_df = (
    items_products_df
    .groupBy("order_id")
    .agg(
        avg("product_weight_g").alias("avg_product_weight_g"),
        avg("product_volume_cm3").alias("avg_product_volume_cm3"),
    )
)


orders_enriched_df = (
    orders_enriched_df
    .join(
        product_metrics_df,
        on="order_id",
        how="left",
    )
)

category_by_order_df = (
    items_products_df
    .groupBy(
        "order_id",
        "product_category_name_english",
    )
    .agg(
        count("*").alias("category_item_count"),
        spark_sum("price").alias("category_total_price"),
        spark_sum("freight_value").alias("category_total_freight"),
    )
)


category_window = (
    Window
    .partitionBy("order_id")
    .orderBy(
        col("category_item_count").desc(),
        col("category_total_price").desc(),
        col("product_category_name_english").asc(),
    )
)

main_category_df = (
    category_by_order_df
    .withColumn(
        "category_rank",
        row_number().over(category_window),
    )
    .filter(col("category_rank") == 1)
    .select(
        "order_id",
        col("product_category_name_english").alias(
            "main_product_category"
        ),
    )
)

orders_enriched_df = (
    orders_enriched_df
    .join(
        main_category_df,
        on="order_id",
        how="left",
    )
)


reviews_agg_df = (
    reviews_df
    .groupBy("order_id")
    .agg(
        avg("review_score").alias("avg_review_score"),
        count("*").alias("review_count"),
        spark_max(
            when(
                col("review_comment_message").isNotNull(),
                1,
            ).otherwise(0)
        ).alias("has_review_comment"),
    )
)

orders_enriched_df = (
    orders_enriched_df
    .withColumn(
        "purchase_year",
        year(col("order_purchase_timestamp")),
    )
    .withColumn(
        "purchase_month",
        month(col("order_purchase_timestamp")),
    )
    .withColumn(
        "purchase_day_of_week",
        dayofweek(col("order_purchase_timestamp")),
    )
)

orders_analytics_df = (
    orders_enriched_df
    .join(
        reviews_agg_df,
        on="order_id",
        how="left",
    )
)


orders_analytics_df = orders_analytics_df.select(
    "order_id",
    "customer_unique_id",
    "customer_state",
    "customer_city",
    "order_status",
    "order_purchase_timestamp",
    "order_estimated_delivery_date",
    "order_delivered_customer_date",
    "purchase_year",
    "purchase_month",
    "purchase_day_of_week",
    "item_count",
    "product_count",
    "seller_count",
    "total_price",
    "total_freight",
    "order_value",
    "main_product_category",
    "avg_product_weight_g",
    "avg_product_volume_cm3",
    "estimated_delivery_days",
    "actual_delivery_days",
    "delay_days",
    "is_delayed",
    "avg_review_score",
    "review_count",
    "has_review_comment",
)

category_order_info_df = orders_analytics_df.select(
    "order_id",
    "customer_state",
    "order_purchase_timestamp",
    "purchase_year",
    "purchase_month",
    "estimated_delivery_days",
    "actual_delivery_days",
    "delay_days",
    "is_delayed",
    "avg_review_score",
)

## contruccion tables
category_analytics_df = (
    category_by_order_df
    .join(
        category_order_info_df,
        on="order_id",
        how="left",
    )
)

category_analytics_df = category_analytics_df.select(
    "order_id",
    "product_category_name_english",
    "customer_state",
    "order_purchase_timestamp",
    "purchase_year",
    "purchase_month",
    "category_item_count",
    "category_total_price",
    "category_total_freight",
    "estimated_delivery_days",
    "actual_delivery_days",
    "delay_days",
    "is_delayed",
    "avg_review_score",
)

print(
    "orders_analytics_df:",
    orders_analytics_df.count(),
)

print(
    "category_analytics_df:",
    category_analytics_df.count(),
)

orders_analytics_df.select(
    "order_id",
    "main_product_category",
    "is_delayed",
    "avg_review_score",
    "review_count",
    "has_review_comment",
).show(20, truncate=False)

orders_enriched_df.printSchema()

print("orders_df:", orders_df.count())
print("orders_enriched_df:", orders_enriched_df.count())

# persist results
orders_output_path = (
    "s3://aws-olist-data-pipeline/"
    "processed/orders_analytics/"
)

category_output_path = (
    "s3://aws-olist-data-pipeline/"
    "processed/category_analytics/"
)


(
    orders_analytics_df
    .write
    .mode("overwrite")
    .parquet(orders_output_path)
)

(
    category_analytics_df
    .write
    .mode("overwrite")
    .parquet(category_output_path)
)

print("[OK] orders_analytics written to S3")
print("[OK] category_analytics written to S3")

job.commit()