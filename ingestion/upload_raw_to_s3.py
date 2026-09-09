from pathlib import Path

import boto3
from botocore.exceptions import BotoCoreError, ClientError


BUCKET_NAME = "aws-olist-data-pipeline"

# ruta donde estan los CSV dentro del repo.
DATA_DIR = Path("data")

FILES_TO_UPLOAD = {
    "olist_customers_dataset.csv": "raw/customers/olist_customers_dataset.csv",
    "olist_orders_dataset.csv": "raw/orders/olist_orders_dataset.csv",
    "olist_order_items_dataset.csv": "raw/order_items/olist_order_items_dataset.csv",
    "olist_order_reviews_dataset.csv": "raw/order_reviews/olist_order_reviews_dataset.csv",
    "olist_products_dataset.csv": "raw/products/olist_products_dataset.csv",
    "olist_sellers_dataset.csv": "raw/sellers/olist_sellers_dataset.csv",
    "product_category_name_translation.csv": (
        "raw/category_translation/product_category_name_translation.csv"
    ),
}


def upload_files() -> None:
    s3 = boto3.client("s3")

    for filename, s3_key in FILES_TO_UPLOAD.items():
        local_file = DATA_DIR / filename

        if not local_file.exists():
            print(f"[SKIP] No se encontró: {local_file}")
            continue

        try:
            print(f"[UPLOAD] {local_file} -> s3://{BUCKET_NAME}/{s3_key}")

            s3.upload_file(
                Filename=str(local_file),
                Bucket=BUCKET_NAME,
                Key=s3_key,
            )

            print(f"[OK] {filename}")

        except (BotoCoreError, ClientError) as error:
            print(f"[ERROR] No se pudo subir {filename}")
            print(error)


if __name__ == "__main__":
    upload_files()