import sys
import time
from pathlib import Path

import boto3


DATABASE = "olist_analytics_db"
OUTPUT_LOCATION = (
    "s3://aws-olist-data-pipeline/"
    "athena-results/"
)
REGION = "us-east-1"


athena = boto3.client(
    "athena",
    region_name=REGION,
)


def read_query(query_path: str) -> str:
    path = Path(query_path)

    if not path.exists():
        raise FileNotFoundError(
            f"Query file not found: {path}"
        )

    return path.read_text(
        encoding="utf-8"
    )


def start_query(query: str) -> str:
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            "Database": DATABASE,
        },
        ResultConfiguration={
            "OutputLocation": OUTPUT_LOCATION,
        },
    )

    return response["QueryExecutionId"]


def wait_for_query(
    query_execution_id: str,
) -> None:

    while True:
        response = athena.get_query_execution(
            QueryExecutionId=query_execution_id
        )

        status = response[
            "QueryExecution"
        ]["Status"]

        state = status["State"]

        print(f"[STATUS] {state}")

        if state == "SUCCEEDED":
            return

        if state in {
            "FAILED",
            "CANCELLED",
        }:
            reason = status.get(
                "StateChangeReason",
                "Unknown error",
            )

            raise RuntimeError(
                f"Query {state}: {reason}"
            )

        time.sleep(2)


def print_results(
    query_execution_id: str,
) -> None:

    response = athena.get_query_results(
        QueryExecutionId=query_execution_id
    )

    rows = response[
        "ResultSet"
    ]["Rows"]

    if not rows:
        print("[INFO] Query returned no rows.")
        return

    values = []

    for row in rows:
        values.append(
            [
                item.get("VarCharValue", "NULL")
                for item in row["Data"]
            ]
        )

    headers = values[0]
    data = values[1:]

    widths = [
        max(
            len(str(row[i]))
            for row in [headers] + data
        )
        for i in range(len(headers))
    ]

    header_line = " | ".join(
        value.ljust(width)
        for value, width
        in zip(headers, widths)
    )

    print("\n" + header_line)

    print(
        "-+-".join(
            "-" * width
            for width in widths
        )
    )

    for row in data:
        print(
            " | ".join(
                value.ljust(width)
                for value, width
                in zip(row, widths)
            )
        )


def main() -> None:

    if len(sys.argv) != 2:
        print(
            "Usage:"
            "\npython athena/run_query.py "
            "<query.sql>"
        )
        sys.exit(1)

    query_path = sys.argv[1]

    query = read_query(query_path)

    print(f"[QUERY] {query_path}")

    execution_id = start_query(query)

    print(
        f"[EXECUTION ID] {execution_id}"
    )

    wait_for_query(execution_id)

    print("[OK] Query succeeded.")

    print_results(execution_id)


if __name__ == "__main__":
    main()