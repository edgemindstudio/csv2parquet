# src/csv2parquet/transform.py

from __future__ import annotations

import hashlib
import logging
import os
import time

from .io import read_csv, validate_rows, write_parquet_partitioned

log = logging.getLogger(__name__)


def file_fingerprint(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()[:16]


def _marker_path(run_state_dir: str, fp: str) -> str:
    return os.path.join(run_state_dir, f"{fp}.done")


def already_processed(run_state_dir: str, fp: str) -> bool:
    os.makedirs(run_state_dir, exist_ok=True)
    return os.path.exists(_marker_path(run_state_dir, fp))


def mark_done(run_state_dir: str, fp: str) -> None:
    os.makedirs(run_state_dir, exist_ok=True)
    open(_marker_path(run_state_dir, fp), "w").close()


def csv_to_parquet(
    input_csv: str, dest_prefix: str, dlq_path: str, run_state_dir: str
) -> dict[str, object]:
    """Core pipeline: validate → (DLQ, good) → write Parquet partitions → idempotency."""
    fp = file_fingerprint(input_csv)
    run_id = f"run-{int(time.time())}-{fp}"

    # Log the skip case
    if already_processed(run_state_dir, fp):
        payload: dict[str, object] = {
            "status": "skipped",
            "reason": "idempotent",
            "fingerprint": fp,
            "run_id": run_id,
        }
        log.info("run_summary", extra=payload)
        return payload

    raw = read_csv(input_csv)
    good, bad = validate_rows(raw)

    if not bad.empty:
        os.makedirs(os.path.dirname(dlq_path), exist_ok=True)
        bad.to_csv(dlq_path, index=False)
        log.warning("bad_rows", extra={"count": int(len(bad)), "dlq": dlq_path})

    written = write_parquet_partitioned(good, dest_prefix)
    mark_done(run_state_dir, fp)

    summary: dict[str, object] = {
        "status": "ok",
        "rows_in": int(len(raw)),
        "rows_out": int(written),
        "fingerprint": fp,
        "run_id": run_id,
        "output_prefix": dest_prefix,
    }
    log.info("run_summary", extra=summary)
    return summary
