import boto3
from botocore.exceptions import ClientError


DATABASE_NAME = "olist_raw_db"

glue = boto3.client("glue")


ORDERS_COLUMNS = [
    {"Name": "order_id", "Type": "string"},
    {"Name": "customer_id", "Type": "string"},
    {"Name": "order_status", "Type": "string"},
    {"Name": "order_purchase_timestamp", "Type": "string"},
    {"Name": "order_approved_at", "Type": "string"},
    {"Name": "order_delivered_carrier_date", "Type": "string"},
    {"Name": "order_delivered_customer_date", "Type": "string"},
    {"Name": "order_estimated_delivery_date", "Type": "string"},
]


CATEGORY_COLUMNS = [
    {"Name": "product_category_name", "Type": "string"},
    {"Name": "product_category_name_english", "Type": "string"},
]


def build_table_input(
    table_name: str,
    location: str,
    columns: list[dict[str, str]],
) -> dict:
    return {
        "Name": table_name,
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {
            "classification": "csv",
            "skip.header.line.count": "1",
            "typeOfData": "file",
        },
        "StorageDescriptor": {
            "Columns": columns,
            "Location": location,
            "InputFormat": (
                "org.apache.hadoop.mapred.TextInputFormat"
            ),
            "OutputFormat": (
                "org.apache.hadoop.hive.ql.io."
                "HiveIgnoreKeyTextOutputFormat"
            ),
            "SerdeInfo": {
                "SerializationLibrary": (
                    "org.apache.hadoop.hive.serde2.OpenCSVSerde"
                ),
                "Parameters": {
                    "separatorChar": ",",
                    "quoteChar": "\"",
                },
            },
        },
    }


def create_or_update_table(
    table_name: str,
    location: str,
    columns: list[dict[str, str]],
) -> None:

    table_input = build_table_input(
        table_name=table_name,
        location=location,
        columns=columns,
    )

    try:
        glue.get_table(
            DatabaseName=DATABASE_NAME,
            Name=table_name,
        )

        glue.update_table(
            DatabaseName=DATABASE_NAME,
            TableInput=table_input,
        )

        print(f"[UPDATED] {table_name}")

    except glue.exceptions.EntityNotFoundException:

        glue.create_table(
            DatabaseName=DATABASE_NAME,
            TableInput=table_input,
        )

        print(f"[CREATED] {table_name}")


def main() -> None:

    create_or_update_table(
        table_name="orders",
        location="s3://aws-olist-data-pipeline/raw/orders/",
        columns=ORDERS_COLUMNS,
    )

    create_or_update_table(
        table_name="category_translation",
        location=(
            "s3://aws-olist-data-pipeline/"
            "raw/category_translation/"
        ),
        columns=CATEGORY_COLUMNS,
    )


if __name__ == "__main__":
    main()