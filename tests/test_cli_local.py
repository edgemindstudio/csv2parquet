# tests/test_cli_local.py

import glob
import sys
from pathlib import Path

import pandas as pd
import pytest


def test_cli_local(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
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

    out = tmp_path / "bronze" / "orders"
    dlq = tmp_path / "dlq.csv"
    state = tmp_path / ".state"

    args = [
        "csv2parquet",
        "--input-csv",
        str(csv),
        "--s3-prefix",
        str(out),
        "--dlq-path",
        str(dlq),
        "--state-dir",
        str(state),
    ]
    monkeypatch.setattr(sys, "argv", args)

    from csv2parquet.cli import main

    main()
    files = glob.glob(str(out / "YYYY=*/MM=*/DD=*/*.parquet"))
    assert len(files) == 1
