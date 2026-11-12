"""Generate small realistic 'good' monthly files and the 8 defect types.

Used by the tests (so CI never downloads real data) and by the demo, which applies
the same defects to real TLC files.
"""

from __future__ import annotations

import random
from datetime import datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from dpp import months

DEFECTS = [
    "missing_column",  # 1. required column dropped
    "retyped_column",  # 2. fare_amount arrives as text
    "negative_fares",  # 3. 5% of rows get a negated fare
    "wrong_month",  # 4. every timestamp is from a different month
    "row_collapse",  # 5. 80% of rows missing
    "duplicated_rows",  # 6. every row sent twice
    "corrupt_file",  # 7. truncated parquet
    "extra_column",  # 8. harmless new column (should only warn)
]

# Real-ish payment mix observed in Jan 2025: 0=flex fare, 1=card, 2=cash, 3=no charge, 4=dispute
_PAYMENT_WEIGHTS = {0: 0.15, 1: 0.70, 2: 0.11, 3: 0.01, 4: 0.03}


def make_good_month(month: str, n: int = 5000, seed: int = 0, n_bad_rows: int = 3) -> pa.Table:
    rng = random.Random(f"{month}-{seed}")
    start = datetime.combine(months.parse(month), datetime.min.time())
    seconds = int((datetime.combine(months.parse(months.add(month, 1)), datetime.min.time()) - start).total_seconds())
    cols: dict[str, list] = {k: [] for k in [
        "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count", "trip_distance",
        "RatecodeID", "store_and_fwd_flag", "PULocationID", "DOLocationID", "payment_type", "fare_amount",
        "extra", "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge", "total_amount",
        "congestion_surcharge", "Airport_fee",
    ]}  # fmt: skip
    for i in range(n):
        pu = start + timedelta(seconds=rng.randrange(seconds - 7200))
        do = pu + timedelta(seconds=rng.randrange(120, 3600))
        if i < n_bad_rows:  # a few rows that break a row rule, well under the 0.5% limit
            do = pu - timedelta(minutes=5)
        pt = rng.choices(list(_PAYMENT_WEIGHTS), weights=list(_PAYMENT_WEIGHTS.values()))[0]
        dist = round(rng.uniform(0.3, 20), 2)
        fare = round(3 + dist * 2.5, 2)
        if pt in (3, 4) and rng.random() < 0.4:
            fare = -fare  # refunds / disputes: legitimately negative
        tip = round(fare * 0.2, 2) if pt == 1 and fare > 0 else 0.0
        flex = pt == 0
        cols["VendorID"].append(rng.choice([1, 2, 7]))
        cols["tpep_pickup_datetime"].append(pu)
        cols["tpep_dropoff_datetime"].append(do)
        cols["passenger_count"].append(None if flex else rng.randint(1, 4))
        cols["trip_distance"].append(dist)
        cols["RatecodeID"].append(None if flex else 1)
        cols["store_and_fwd_flag"].append(None if flex else "N")
        cols["PULocationID"].append(rng.randint(1, 265))
        cols["DOLocationID"].append(rng.randint(1, 265))
        cols["payment_type"].append(pt)
        cols["fare_amount"].append(fare)
        cols["extra"].append(1.0)
        cols["mta_tax"].append(0.5)
        cols["tip_amount"].append(tip)
        cols["tolls_amount"].append(0.0)
        cols["improvement_surcharge"].append(1.0)
        cols["total_amount"].append(round(fare + tip + 2.5, 2))
        cols["congestion_surcharge"].append(None if flex else 2.5)
        cols["Airport_fee"].append(None if flex else 0.0)

    schema = pa.schema([
        ("VendorID", pa.int32()), ("tpep_pickup_datetime", pa.timestamp("us")),
        ("tpep_dropoff_datetime", pa.timestamp("us")), ("passenger_count", pa.int64()),
        ("trip_distance", pa.float64()), ("RatecodeID", pa.int64()), ("store_and_fwd_flag", pa.large_string()),
        ("PULocationID", pa.int32()), ("DOLocationID", pa.int32()), ("payment_type", pa.int64()),
        *[(c, pa.float64()) for c in ["fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
                                      "improvement_surcharge", "total_amount", "congestion_surcharge",
                                      "Airport_fee"]],
    ])  # fmt: skip
    table = pa.table(cols, schema=schema)
    if month >= "2025-01":  # TLC added this column in January 2025
        table = table.append_column("cbd_congestion_fee", pa.array([0.75] * n, pa.float64()))
    return table


def apply_defect(table: pa.Table, defect: str, seed: int = 0) -> pa.Table:
    """Return a damaged copy of `table`. ('corrupt_file' is handled in write_defect.)"""
    rng = random.Random(seed)
    n = table.num_rows
    if defect == "missing_column":
        return table.drop_columns(["fare_amount"])
    if defect == "retyped_column":
        i = table.schema.get_field_index("fare_amount")
        as_text = pc.cast(table["fare_amount"], pa.string())
        return table.set_column(i, pa.field("fare_amount", pa.string()), as_text)
    if defect == "negative_fares":
        hit = set(rng.sample(range(n), k=int(n * 0.05)))
        fares = table["fare_amount"].to_pylist()
        new = [-abs(f) if i in hit and f is not None else f for i, f in enumerate(fares)]
        i = table.schema.get_field_index("fare_amount")
        return table.set_column(i, "fare_amount", pa.array(new, pa.float64()))
    if defect == "wrong_month":
        shift = pa.scalar(timedelta(days=-90), pa.duration("us"))
        for col in ["tpep_pickup_datetime", "tpep_dropoff_datetime"]:
            i = table.schema.get_field_index(col)
            table = table.set_column(i, col, pc.add(table[col], shift))
        return table
    if defect == "row_collapse":
        return table.slice(0, n // 5)
    if defect == "duplicated_rows":
        return pa.concat_tables([table, table])
    if defect == "extra_column":
        return table.append_column("promo_code", pa.array(["NONE"] * n, pa.large_string()))
    if defect == "corrupt_file":
        return table
    raise ValueError(f"unknown defect {defect!r}")


def write_defect(table: pa.Table, defect: str, path: Path, seed: int = 0) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(apply_defect(table, defect, seed), path)
    if defect == "corrupt_file":
        data = path.read_bytes()
        path.write_bytes(data[: len(data) // 2])  # lose the footer, like an interrupted upload
    return path


def write_good(month: str, path: Path, n: int = 5000, seed: int = 0) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(make_good_month(month, n=n, seed=seed), path)
    return path
