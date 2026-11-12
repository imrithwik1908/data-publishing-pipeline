"""Step 2 - WRITE: load the landed file into a typed staging table.

Every contract column is cast explicitly (TRY_CAST, so bad values become NULL and get
caught by the null-rate checks instead of crashing the load). Each row is tagged with
the first row rule it breaks, or NULL if it is valid.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from dpp import months
from dpp.contract import Contract


@dataclass
class SourceSchema:
    """What the file actually contains, mapped onto the contract."""

    types: dict[str, pa.DataType]  # source column name -> arrow type
    mapping: dict[str, str] = field(default_factory=dict)  # contract name -> source name
    extra: list[str] = field(default_factory=list)  # source columns not in the contract


def read_source_schema(path: Path, contract: Contract) -> SourceSchema:
    """Raises if the file is not readable Parquet (corrupt / truncated)."""
    schema = pq.read_schema(path)
    _ = pq.ParquetFile(path).metadata  # reads the footer: catches truncated files early
    types = dict(zip(schema.names, schema.types, strict=True))
    src = SourceSchema(types=types)
    for name in schema.names:
        col = next((c for c in contract.columns if c.matches(name)), None)
        if col is None:
            src.extra.append(name)
        else:
            src.mapping[col.name] = name
    return src


def staging_table(batch_id: int) -> str:
    return f"stg_trips__{batch_id}"


def rule_sql(contract: Contract, month: str) -> list[tuple[str, str]]:
    start, end = months.parse(month), months.parse(months.add(month, 1))
    return [
        (r.name, r.sql.format(month_start=f"{start} 00:00:00", month_end=f"{end} 00:00:00")) for r in contract.row_rules
    ]


def load_staging(
    con: duckdb.DuckDBPyConnection,
    path: Path,
    contract: Contract,
    src: SourceSchema,
    batch_id: int,
    month: str,
) -> str:
    table = staging_table(batch_id)
    selects = []
    for c in contract.columns:
        if c.name in src.mapping:
            selects.append(f'TRY_CAST("{src.mapping[c.name]}" AS {c.type}) AS "{c.name}"')
        else:
            selects.append(f'CAST(NULL AS {c.type}) AS "{c.name}"')
    reject_case = " ".join(
        f"WHEN NOT coalesce(({sql}), false) THEN '{name}'" for name, sql in rule_sql(contract, month)
    )
    con.execute(f"DROP TABLE IF EXISTS {table}")
    con.execute(
        f"""
        CREATE TABLE {table} AS
        SELECT *,
               DATE '{month}-01' AS pickup_month,
               CASE {reject_case} ELSE NULL END AS _reject_reason
        FROM (SELECT {", ".join(selects)} FROM read_parquet(?))
        """,
        [str(path)],
    )
    return table
