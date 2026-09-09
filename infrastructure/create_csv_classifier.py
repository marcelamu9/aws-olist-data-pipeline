import boto3
from botocore.exceptions import ClientError


CLASSIFIER_NAME = "olist_csv_with_header"
CRAWLER_NAME = "olist_raw_crawler"


glue = boto3.client("glue")


def create_classifier() -> None:
    try:
        glue.get_classifier(Name=CLASSIFIER_NAME)
        print(f"[OK] Classifier already exists: {CLASSIFIER_NAME}")

    except glue.exceptions.EntityNotFoundException:
        glue.create_classifier(
            CsvClassifier={
                "Name": CLASSIFIER_NAME,
                "Delimiter": ",",
                "QuoteSymbol": '"',
                "ContainsHeader": "PRESENT",
                "DisableValueTrimming": False,
            }
        )

        print(f"[CREATED] Classifier: {CLASSIFIER_NAME}")


def update_crawler() -> None:
    crawler = glue.get_crawler(Name=CRAWLER_NAME)["Crawler"]

    glue.update_crawler(
        Name=CRAWLER_NAME,
        Role=crawler["Role"],
        DatabaseName=crawler["DatabaseName"],
        Targets=crawler["Targets"],
        Classifiers=[CLASSIFIER_NAME],
        SchemaChangePolicy={
            "UpdateBehavior": "UPDATE_IN_DATABASE",
            "DeleteBehavior": "LOG",
        },
    )

    print(f"[UPDATED] Crawler: {CRAWLER_NAME}")


def main() -> None:
    try:
        create_classifier()
        update_crawler()

        print("\nClassifier configured.")
        print("Run the crawler again to refresh the schemas.")

    except ClientError as error:
        print("[ERROR]")
        print(error)


if __name__ == "__main__":
    main()