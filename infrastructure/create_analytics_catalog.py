import time

import boto3


DATABASE_NAME = "olist_analytics_db"
CRAWLER_NAME = "olist_analytics_crawler"
ROLE_NAME = "AWSGlueServiceRole-OlistPipeline"

S3_TARGETS = [
    {
        "Path": (
            "s3://aws-olist-data-pipeline/"
            "processed/orders_analytics/"
        )
    },
    {
        "Path": (
            "s3://aws-olist-data-pipeline/"
            "processed/category_analytics/"
        )
    },
]


glue = boto3.client("glue")
iam = boto3.client("iam")


def get_role_arn() -> str:
    response = iam.get_role(RoleName=ROLE_NAME)
    return response["Role"]["Arn"]


def create_database() -> None:
    try:
        glue.get_database(Name=DATABASE_NAME)
        print(f"[OK] Database already exists: {DATABASE_NAME}")

    except glue.exceptions.EntityNotFoundException:
        glue.create_database(
            DatabaseInput={
                "Name": DATABASE_NAME,
                "Description": (
                    "Processed analytical datasets "
                    "for Olist e-commerce project"
                ),
            }
        )

        print(f"[CREATED] Database: {DATABASE_NAME}")


def create_or_update_crawler(role_arn: str) -> None:
    try:
        glue.get_crawler(Name=CRAWLER_NAME)

        glue.update_crawler(
            Name=CRAWLER_NAME,
            Role=role_arn,
            DatabaseName=DATABASE_NAME,
            Targets={
                "S3Targets": S3_TARGETS
            },
            SchemaChangePolicy={
                "UpdateBehavior": "UPDATE_IN_DATABASE",
                "DeleteBehavior": "LOG",
            },
        )

        print(f"[UPDATED] Crawler: {CRAWLER_NAME}")

    except glue.exceptions.EntityNotFoundException:
        glue.create_crawler(
            Name=CRAWLER_NAME,
            Role=role_arn,
            DatabaseName=DATABASE_NAME,
            Description=(
                "Crawler for processed Olist "
                "analytics datasets"
            ),
            Targets={
                "S3Targets": S3_TARGETS
            },
            SchemaChangePolicy={
                "UpdateBehavior": "UPDATE_IN_DATABASE",
                "DeleteBehavior": "LOG",
            },
        )

        print(f"[CREATED] Crawler: {CRAWLER_NAME}")


def run_crawler() -> None:
    crawler = glue.get_crawler(
        Name=CRAWLER_NAME
    )["Crawler"]

    if crawler["State"] != "READY":
        print(
            f"[INFO] Crawler state: "
            f"{crawler['State']}"
        )
        return

    glue.start_crawler(Name=CRAWLER_NAME)
    print("[STARTED] Analytics crawler")

    while True:
        response = glue.get_crawler(
            Name=CRAWLER_NAME
        )

        state = response["Crawler"]["State"]

        print(f"[WAIT] {state}")

        if state == "READY":
            last_crawl = response["Crawler"].get(
                "LastCrawl",
                {}
            )

            print(
                "[DONE]",
                last_crawl.get("Status"),
            )
            break

        time.sleep(10)


def list_tables() -> None:
    response = glue.get_tables(
        DatabaseName=DATABASE_NAME
    )

    print("\nAnalytics tables:")

    for table in response["TableList"]:
        print(f" - {table['Name']}")


def main() -> None:
    role_arn = get_role_arn()

    create_database()
    create_or_update_crawler(role_arn)
    run_crawler()
    list_tables()


if __name__ == "__main__":
    main()