# src/csv2parquet/cli.py

import argparse

from .logging_conf import configure_logging
from .transform import csv_to_parquet


def main() -> None:
    """CSV → Parquet on S3/local with validation & idempotency."""
    configure_logging()
    ap = argparse.ArgumentParser(
        description="CSV → Parquet on S3/local with validation & idempotency."
    )
    ap.add_argument("--input-csv", required=True)
    ap.add_argument("--s3-prefix", required=True, help="s3://bucket/prefix OR local folder")
    ap.add_argument("--dlq-path", default="../../datasets/dlq/bad_rows.csv")
    ap.add_argument("--state-dir", default=".state")
    args = ap.parse_args()
    csv_to_parquet(args.input_csv, args.s3_prefix, args.dlq_path, args.state_dir)


if __name__ == "__main__":
    main()
