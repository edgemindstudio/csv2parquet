![CI](https://github.com/edgemindstudio/csv2parquet/actions/workflows/ci.yml/badge.svg)

# CSV → Parquet (Week 1)

A small, production-style Python CLI that validates CSV “orders” data and writes **partitioned Parquet** (YYYY/MM/DD) to **local disk or S3**. It includes **idempotent** re-runs, a **DLQ** for bad rows, **structured JSON logs**, and full **CI quality gates** (ruff, black, mypy, pytest with ~99% coverage). Also ships six DuckDB **window-function** exercises saved as Parquet.

---

## Features

- **CSV → Parquet** with partitioning by `YYYY/MM/DD` (PyArrow)
- **Schema enforcement** (Pydantic) with a **DLQ** (`datasets/dlq/bad_rows.csv`)
- **Idempotency** via content fingerprint markers in `.state/`
- **Structured JSON logging** (python-json-logger) with run metadata
- **Local & S3** destinations (S3 tested with `moto`, optional real run)
- **Quality gates**: ruff, black, mypy, pytest (coverage threshold ≥ 80%)
- **SQL reps**: 6 DuckDB window queries saved to `datasets/sql_practice/results/`

---

## Tech Stack

Python 3.12 · Poetry · Pandas · PyArrow · Pydantic · DuckDB · boto3/s3fs · PyTest · Mypy · Ruff · Black · pre-commit · GitHub Actions

---

## Project Layout

```kotlin
├─ src/csv2parquet/
│ ├─ init.py
│ ├─ cli.py # CLI entrypoint
│ ├─ io.py # schema, read/validate, write (local/S3)
│ ├─ logging_conf.py # JSON logging
│ └─ transform.py # idempotency + orchestration
├─ tests/ # unit/integration tests (incl. moto S3)
├─ datasets/
│ ├─ synthetic_orders/ # input CSV (local dev)
│ ├─ sql_practice/results/ # DuckDB window-query outputs (Parquet)
│ └─ dlq/ # bad_rows.csv (DLQ)
├─ .github/workflows/ci.yml # lint, typecheck, tests, coverage
├─ pyproject.toml # Poetry project & deps
├─ mypy.ini # strict type-check config
├─ .pre-commit-config.yaml # ruff/black/mypy hooks
└─ README.md
```
> Note: `datasets/`, `bronze/`, `.state/`, and `*.parquet` are git-ignored.

---
## Quickstart

### Prereqs
- Python **3.12**
- Poetry **2.x** (`pipx install poetry`)

### Install
```bash
    poetry install
    pre-commit install
```
## Generate a sample CSV (local dev)
```bash
    python - <<'PY'
    import pandas as pd, random, datetime as dt, os
    os.makedirs("datasets/synthetic_orders", exist_ok=True)
    rows=[]
    for i in range(200):
        rows.append({
          "order_id": f"o{i:05d}",
          "user_id": f"u{random.randint(1,30):04d}",
          "ts": (dt.date(2024,1,1)+dt.timedelta(days=random.randint(0,60))).isoformat(),
          "item": random.choice(["book","pen","tablet"]),
          "qty": random.randint(1,5),
          "price": round(random.uniform(3,99),2),
          "country": random.choice(["US","CA","NG","ZA","UK"])
        })
    pd.DataFrame(rows).to_csv("datasets/synthetic_orders/orders.csv", index=False)
    print("Wrote datasets/synthetic_orders/orders.csv")
    PY
```
## Run (local target)
```bash
    poetry run csv2parquet \
      --input-csv datasets/synthetic_orders/orders.csv \
      --s3-prefix ./bronze/orders \
      --dlq-path datasets/dlq/bad_rows.csv \
      --state-dir .state
```
Re-run the exact command to prove idempotency (second run logs "status": "skipped" and no new files are created).

List a few output files:
```bash
    find bronze/orders -maxdepth 4 -type f | head
```
## Run (S3 target, optional)
```bash
    # Requires AWS creds and a unique bucket name, e.g. edgemind-data-dev-123
    poetry run csv2parquet \
      --input-csv datasets/synthetic_orders/orders.csv \
      --s3-prefix s3://<your-bucket>/bronze/orders \
      --dlq-path datasets/dlq/bad_rows.csv \
      --state-dir .state
```
## CLI
```text
csv2parquet --input-csv PATH --s3-prefix DEST [--dlq-path PATH] [--state-dir PATH]

--input-csv   Path to a CSV file to ingest.
--s3-prefix   Local folder (e.g., ./bronze/orders) or s3://bucket/prefix.
--dlq-path    Where to write bad rows CSV (default: datasets/dlq/bad_rows.csv).
--state-dir   Idempotency markers (default: .state).
```
## Data Quality & DLQ
- Schema enforced with Pydantic (`qty` must be > 0, types validated).
- Invalid rows are written to `datasets/dlq/bad_rows.csv` with a single-line `_error` message.

Inject a bad row to see the DLQ:
```bash
    printf "order_id,user_id,ts,item,qty,price,country\no_bad,u1,2024-01-02,book,0,9.99,US\n" > /tmp/bad.csv
    poetry run csv2parquet \
      --input-csv /tmp/bad.csv \
      --s3-prefix ./bronze/orders \
      --dlq-path datasets/dlq/bad_rows.csv \
      --state-dir .state
    tail -n +1 datasets/dlq/bad_rows.csv
```
## DuckDB SQL Exercises (saved as Parquet)

Six window queries demonstrating analytics patterns are saved under `datasets/sql_practice/results/`:

- `q1_running_total.parquet`
- `q2_rev7d.parquet`
- `q3_rank_items.parquet`
- `q4_p90_country.parquet`
- `q5_top_buyers.parquet`
- `q6_gap_days.parquet`

Regenerate (optional):
```bash
    python - <<'PY'
    import duckdb, os
    os.makedirs("datasets/sql_practice/results", exist_ok=True)
    con = duckdb.connect()
    con.execute("CREATE OR REPLACE TABLE orders AS SELECT * FROM read_parquet('bronze/orders/**/*.parquet')")

    def save(q, out): con.execute(f"COPY ({q}) TO '{out}' (FORMAT PARQUET)")

    save("""
    SELECT user_id, ts,
           SUM(price*qty) OVER (PARTITION BY user_id ORDER BY CAST(ts AS DATE)
             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_spend
    FROM orders
    """, "datasets/sql_practice/results/q1_running_total.parquet")

    save("""
    SELECT ts, SUM(price*qty) AS revenue,
           SUM(SUM(price*qty)) OVER (ORDER BY CAST(ts AS DATE)
             ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rev_7d
    FROM orders GROUP BY ts ORDER BY ts
    """, "datasets/sql_practice/results/q2_rev7d.parquet")

    save("""
    SELECT ts, item, SUM(price*qty) AS rev,
           RANK() OVER (PARTITION BY ts ORDER BY SUM(price*qty) DESC) AS rnk
    FROM orders GROUP BY ts, item
    """, "datasets/sql_practice/results/q3_rank_items.parquet")

    save("""
    SELECT country, percentile_cont(0.9) WITHIN GROUP (ORDER BY price*qty) AS p90
    FROM orders GROUP BY country
    """, "datasets/sql_practice/results/q4_p90_country.parquet")

    save("""
    SELECT strftime(CAST(ts AS DATE), '%Y-%m') AS ym, user_id,
           SUM(price*qty) AS spend,
           DENSE_RANK() OVER (PARTITION BY strftime(CAST(ts AS DATE), '%Y-%m')
                              ORDER BY SUM(price*qty) DESC) AS dr
    FROM orders GROUP BY ym, user_id
    """, "datasets/sql_practice/results/q5_top_buyers.parquet")

    save("""
    WITH s AS (SELECT user_id, CAST(ts AS DATE) AS d FROM orders)
    SELECT user_id, d, LEAD(d) OVER (PARTITION BY user_id ORDER BY d) - d AS days_to_next
    FROM s
    """, "datasets/sql_practice/results/q6_gap_days.parquet")
    print("Saved q1..q6 Parquet files.")
    PY
```
---
## Development

**Quality gates**
```bash
    poetry run ruff check .
    poetry run black --check .
    poetry run mypy .
    poetry run pytest --cov=csv2parquet --cov-report=term-missing
```
**Pre-commit**
```bash
    pre-commit install
    pre-commit run --all-files
```
## Acceptance Checklist (Week 1)

- CSV → Parquet (partitioned) to `./bronze/orders/YYYY=/MM=/DD=/`
- Idempotent re-run (`"status":"skipped" + .state/<fingerprint>.done`)
- Schema enforced; DLQ written on invalid rows
- Tests green; coverage ≥ 80% (actual ~99%)
- Lint/format/typecheck pass (ruff/black/mypy)
- 6 DuckDB window queries saved as Parquet

## LICENSE

MIT License
Copyright (c) 2025 Edgemind
Permission is hereby granted, free of charge, to any person obtaining a copy...
