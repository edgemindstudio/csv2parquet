from pathlib import Path

import boto3
import pandas as pd
from moto import mock_aws

from csv2parquet.transform import csv_to_parquet


def test_csv_to_parquet_writes_to_s3(tmp_path: Path) -> None:
    with mock_aws():
        bkt = "dev-bucket-123"
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=bkt)

        csv = tmp_path / "orders.csv"
        pd.DataFrame(
            [
                {
                    "order_id": "o1",
                    "user_id": "u1",
                    "ts": "2024-02-03",
                    "item": "x",
                    "qty": 1,
                    "price": 1.0,
                    "country": "US",
                }
            ]
        ).to_csv(csv, index=False)

        prefix = f"s3://{bkt}/bronze/orders"
        dlq = tmp_path / "dlq.csv"
        state = tmp_path / ".state"
        res = csv_to_parquet(str(csv), prefix, str(dlq), str(state))

        assert res["status"] == "ok" and res["rows_out"] == 1
        objs = s3.list_objects_v2(Bucket=bkt, Prefix="bronze/orders/")
        assert objs.get("KeyCount", 0) > 0
