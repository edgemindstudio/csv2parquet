from pathlib import Path

from csv2parquet.transform import csv_to_parquet, file_fingerprint


def test_fingerprint_stable(tmp_path: Path) -> None:
    p = tmp_path / "f.csv"
    p.write_text("a,b\n1,2\n")
    assert file_fingerprint(str(p)) == file_fingerprint(str(p))


def test_idempotency_marker_and_skip(tmp_path: Path) -> None:
    # Make a tiny valid CSV
    csv = tmp_path / "orders.csv"
    csv.write_text("order_id,user_id,ts,item,qty,price,country\no1,u1,2024-02-03,book,1,1.0,US\n")
    dest = tmp_path / "bronze" / "orders"
    dlq = tmp_path / "datasets" / "dlq" / "bad.csv"
    state = tmp_path / ".state"

    res1 = csv_to_parquet(str(csv), str(dest), str(dlq), str(state))
    res2 = csv_to_parquet(str(csv), str(dest), str(dlq), str(state))

    assert res1["status"] == "ok"
    assert res2["status"] == "skipped"
    # marker exists
    from csv2parquet.transform import file_fingerprint as fp

    assert (state / f"{fp(str(csv))}.done").exists()


def test_bad_rows_go_to_dlq(tmp_path: Path) -> None:
    csv = tmp_path / "bad.csv"
    csv.write_text(
        "order_id,user_id,ts,item,qty,price,country\no_bad,u1,2024-02-03,book,0,9.99,US\n"
    )
    dest = tmp_path / "bronze" / "orders"
    dlq = tmp_path / "datasets" / "dlq" / "bad_rows.csv"
    state = tmp_path / ".state"

    res = csv_to_parquet(str(csv), str(dest), str(dlq), str(state))
    assert res["rows_out"] == 0
    assert dlq.exists()
    # DLQ has header + 1 row
    assert sum(1 for _ in open(dlq)) == 2
