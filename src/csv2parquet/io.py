# src/csv2parquet/io.py

from __future__ import annotations

import io as _io
import os
from typing import cast
from uuid import uuid4

import boto3
import pandas as pd
from pydantic import BaseModel, field_validator


def _is_s3(path: str) -> bool:
    return path.startswith("s3://")


def _split_s3_uri(uri: str) -> tuple[str, str]:
    # "s3://bucket/prefix" -> ("bucket", "prefix") ; "s3://bucket" -> ("bucket", "")
    rest = uri[5:]
    parts = rest.split("/", 1)
    bucket = parts[0]
    prefix = parts[1] if len(parts) > 1 else ""
    return bucket, prefix


class Order(BaseModel):  # type: ignore[misc]
    order_id: str
    user_id: str
    ts: str  # ISO8601 date or datetime
    item: str
    qty: int
    price: float
    country: str

    @field_validator("qty")  # type: ignore[misc]
    @classmethod
    def qty_positive(cls, v: int) -> int:
        if v <= 0:
            raise ValueError("qty must be > 0")
        return v


def read_csv(path: str) -> pd.DataFrame:
    return pd.read_csv(path)


def validate_rows(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return (good_rows, bad_rows) after schema checks."""
    good: list[dict[str, object]] = []
    bad: list[dict[str, object]] = []
    records = cast(list[dict[str, object]], df.to_dict(orient="records"))
    for r in records:
        try:
            good.append(Order(**r).model_dump())
        except Exception as e:  # capture reason for DLQ
            r2 = dict(r)
            r2["_error"] = str(e).replace("\n", " | ")
            bad.append(r2)
    return pd.DataFrame(good), pd.DataFrame(bad)


def write_parquet_partitioned(df: pd.DataFrame, dest_prefix: str) -> int:
    """Write partitioned Parquet to local or s3 prefix.

    Layout: {dest_prefix}/YYYY=YYYY/MM=MM/DD=DD/part-<uuid>.parquet
    """
    if df.empty:
        return 0

    dt = pd.to_datetime(df["ts"], errors="coerce")
    df = df.assign(
        YYYY=dt.dt.year.astype("Int64").astype(str),
        MM=dt.dt.month.astype("Int64").astype(int).astype(str).str.zfill(2),
        DD=dt.dt.day.astype("Int64").astype(int).astype(str).str.zfill(2),
    )

    total = 0

    if _is_s3(dest_prefix):
        bucket, base = _split_s3_uri(dest_prefix)
        s3 = boto3.client("s3", region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"))
        for (y, m, d), part in df.groupby(["YYYY", "MM", "DD"], dropna=False):
            key = f"{base}/YYYY={y}/MM={m}/DD={d}/part-{uuid4().hex}.parquet"
            buf = _io.BytesIO()
            part.to_parquet(buf, index=False)  # write in-memory
            buf.seek(0)
            s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())
            total += len(part)
        return total

    # Local filesystem: ensure dirs exist
    for (y, m, d), part in df.groupby(["YYYY", "MM", "DD"], dropna=False):
        out_dir = f"{dest_prefix}/YYYY={y}/MM={m}/DD={d}"
        os.makedirs(out_dir, exist_ok=True)
        out_path = f"{out_dir}/part-{uuid4().hex}.parquet"
        part.to_parquet(out_path, index=False)
        total += len(part)

    return total
