import glob
from pathlib import Path

import pandas as pd

from csv2parquet.io import validate_rows, write_parquet_partitioned


def test_validate_rows_splits_bad() -> None:
    df = pd.DataFrame(
        [
            {
                "order_id": "o1",
                "user_id": "u1",
                "ts": "2024-02-03",
                "item": "x",
                "qty": 1,
                "price": 10.0,
                "country": "US",
            },
            {
                "order_id": "o2",
                "user_id": "u2",
                "ts": "2024-02-03",
                "item": "x",
                "qty": 0,
                "price": 10.0,
                "country": "US",
            },
        ]
    )
    good, bad = validate_rows(df)
    assert len(good) == 1 and len(bad) == 1
    assert "_error" in bad.columns


def test_write_parquet_partitioned_local(tmp_path: Path) -> None:
    df = pd.DataFrame(
        [
            {
                "order_id": "o1",
                "user_id": "u1",
                "ts": "2024-02-03",
                "item": "x",
                "qty": 1,
                "price": 10.0,
                "country": "US",
            },
            {
                "order_id": "o2",
                "user_id": "u2",
                "ts": "2024-02-04",
                "item": "y",
                "qty": 2,
                "price": 12.0,
                "country": "US",
            },
        ]
    )
    dest = tmp_path / "bronze" / "orders"
    n = write_parquet_partitioned(df, str(dest))
    assert n == 2
    files = glob.glob(str(dest / "YYYY=*/MM=*/DD=*/*.parquet"))
    assert len(files) == 2
