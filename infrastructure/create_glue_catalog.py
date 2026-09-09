import time

import boto3
from botocore.exceptions import ClientError


GLUE_DATABASE = "olist_raw_db"
CRAWLER_NAME = "olist_raw_crawler"
ROLE_NAME = "AWSGlueServiceRole-OlistPipeline"
S3_TARGET = "s3://aws-olist-data-pipeline/raw/"

S3_TARGETS = [
    {"Path": "s3://aws-olist-data-pipeline/raw/customers/"},
    {"Path": "s3://aws-olist-data-pipeline/raw/order_items/"},
    {"Path": "s3://aws-olist-data-pipeline/raw/order_reviews/"},
    {"Path": "s3://aws-olist-data-pipeline/raw/products/"},
    {"Path": "s3://aws-olist-data-pipeline/raw/sellers/"},
]

glue = boto3.client("glue")
iam = boto3.client("iam")

def get_role_arn() -> str:
    response = iam.get_role(RoleName=ROLE_NAME)
    return response["Role"]["Arn"]

def create_database() -> None:
    try:
        glue.get_database(Name=GLUE_DATABASE)
        print(f"[OK] Database already exists: {GLUE_DATABASE}")

    except glue.exceptions.EntityNotFoundException:
        glue.create_database(
            DatabaseInput={
                "Name": GLUE_DATABASE,
                "Description": "Raw Olist datasets stored in Amazon S3",
            }
        )
        print(f"[CREATED] Database: {GLUE_DATABASE}")

def create_crawler(role_arn: str) -> None:
    try:
        glue.get_crawler(Name=CRAWLER_NAME)
        print(f"[OK] Crawler already exists: {CRAWLER_NAME}")

    except glue.exceptions.EntityNotFoundException:
        glue.create_crawler(
            Name=CRAWLER_NAME,
            Role=role_arn,
            DatabaseName=GLUE_DATABASE,
            Description="Crawler for raw Olist CSV datasets",
            Targets={
                "S3Targets": [
                    {
                        "Path": S3_TARGETS,
                    }
                ]
            },
            SchemaChangePolicy={
                "UpdateBehavior": "UPDATE_IN_DATABASE",
                "DeleteBehavior": "LOG",
            },
        )

        print(f"[CREATED] Crawler: {CRAWLER_NAME}")

def start_crawler() -> None:
    response = glue.get_crawler(Name=CRAWLER_NAME)
    state = response["Crawler"]["State"]

    if state == "READY":
        glue.start_crawler(Name=CRAWLER_NAME)
        print("[STARTED] Crawler execution started.")

    else:
        print(f"[INFO] Crawler current state: {state}")

def wait_for_crawler() -> None:
    print("[WAIT] Waiting for crawler to finish...")

    while True:
        response = glue.get_crawler(Name=CRAWLER_NAME)
        state = response["Crawler"]["State"]

        print(f"  State: {state}")

        if state == "READY":
            last_crawl = response["Crawler"].get("LastCrawl")

            if last_crawl:
                status = last_crawl.get("Status")
                print(f"[DONE] Last crawl status: {status}")
            break

        time.sleep(10)

def list_tables() -> None:
    response = glue.get_tables(DatabaseName=GLUE_DATABASE)

    print("\nTables created in Glue Data Catalog:")

    for table in response["TableList"]:
        print(f" - {table['Name']}")


def main() -> None:
    try:
        role_arn = get_role_arn()

        print(f"[ROLE] {role_arn}")

        create_database()
        create_crawler(role_arn)
        start_crawler()
        wait_for_crawler()
        list_tables()

    except ClientError as error:
        print("[ERROR]")
        print(error)


if __name__ == "__main__":
    main()