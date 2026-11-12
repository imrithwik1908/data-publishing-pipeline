"""Step 3 - AUDIT: every check the contract asks for.

Each check returns one or more CheckResult rows. A batch is quarantined if any
result has severity ERROR and status 'fail'.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass

import duckdb
import pyarrow as pa

from dpp import months
from dpp.contract import Contract
from dpp.stage import SourceSchema, rule_sql


@dataclass
class CheckResult:
    check_name: str
    severity: str  # ERROR | WARN
    status: str  # pass | fail
    observed: str = ""
    threshold: str = ""
    message: str = ""

    @property
    def blocking(self) -> bool:
        return self.severity == "ERROR" and self.status == "fail"

    def as_dict(self) -> dict:
        return asdict(self)


@dataclass
class AuditContext:
    con: duckdb.DuckDBPyConnection
    contract: Contract
    table: str  # staging table
    month: str
    batch_id: int
    checksum: str
    mode: str  # run | backfill
    src: SourceSchema


def _ok(failed: bool) -> str:
    return "fail" if failed else "pass"


# ---------- type families: what counts as a "compatible" type change ----------


def arrow_family(t: pa.DataType) -> str:
    if pa.types.is_integer(t):
        return "integer"
    if pa.types.is_floating(t) or pa.types.is_decimal(t):
        return "float"
    if pa.types.is_string(t) or pa.types.is_large_string(t):
        return "string"
    if pa.types.is_timestamp(t) or pa.types.is_date(t):
        return "timestamp"
    return str(t)


def contract_family(duck_type: str) -> str:
    t = duck_type.upper()
    if t in {"TINYINT", "SMALLINT", "INTEGER", "BIGINT"}:
        return "integer"
    if t in {"FLOAT", "DOUBLE", "REAL"} or t.startswith("DECIMAL"):
        return "float"
    if t == "VARCHAR":
        return "string"
    if t in {"TIMESTAMP", "DATE"}:
        return "timestamp"
    return t


NUMERIC = {"integer", "float"}


# ---------- schema ----------


def check_schema(ctx: AuditContext) -> list[CheckResult]:
    c = ctx.contract
    out = []

    missing = [col.name for col in c.columns if col.required and col.name not in ctx.src.mapping]
    out.append(
        CheckResult(
            "schema.missing_columns",
            "ERROR",
            _ok(bool(missing)),
            observed=",".join(missing) or "none",
            threshold="none",
            message=f"required columns missing: {missing}" if missing else "all required columns present",
        )
    )

    sev = c.checks.extra_columns.severity
    out.append(
        CheckResult(
            "schema.extra_columns",
            sev,
            _ok(bool(ctx.src.extra)),
            observed=",".join(ctx.src.extra) or "none",
            threshold="none",
            message=f"columns not in contract (ignored): {ctx.src.extra}" if ctx.src.extra else "no extra columns",
        )
    )

    compatible, incompatible = [], []
    for col in c.columns:
        if col.name not in ctx.src.mapping:
            continue
        got = arrow_family(ctx.src.types[ctx.src.mapping[col.name]])
        want = contract_family(col.type)
        if got == want:
            continue
        desc = f"{col.name}: {got} -> {want}"
        (compatible if {got, want} <= NUMERIC else incompatible).append(desc)

    out.append(
        CheckResult(
            "schema.incompatible_types",
            "ERROR",
            _ok(bool(incompatible)),
            observed="; ".join(incompatible) or "none",
            threshold="none",
            message=f"column types changed incompatibly: {incompatible}" if incompatible else "types compatible",
        )
    )
    out.append(
        CheckResult(
            "schema.retyped_columns",
            c.checks.retyped_columns.severity,
            _ok(bool(compatible)),
            observed="; ".join(compatible) or "none",
            threshold="none",
            message=f"numeric type drift, cast to contract type: {compatible}" if compatible else "no type drift",
        )
    )
    return out


# ---------- column nulls ----------


def check_null_rates(ctx: AuditContext) -> list[CheckResult]:
    cols = [c for c in ctx.contract.columns if c.max_null_rate is not None and c.name in ctx.src.mapping]
    if not cols:
        return []
    exprs = ", ".join(f'avg(("{c.name}" IS NULL)::INT)' for c in cols)
    rates = ctx.con.execute(f"SELECT {exprs} FROM {ctx.table}").fetchone()
    out = []
    for col, rate in zip(cols, rates, strict=True):
        rate = rate or 0.0
        failed = rate > col.max_null_rate
        out.append(
            CheckResult(
                f"null_rate.{col.name}",
                "ERROR",
                _ok(failed),
                observed=f"{rate:.4%}",
                threshold=f"<= {col.max_null_rate:.2%}",
                message=f"{col.name} is null in {rate:.2%} of rows",
            )
        )
    return out


# ---------- row-level rules ----------


def check_row_rules(ctx: AuditContext) -> list[CheckResult]:
    rules = rule_sql(ctx.contract, ctx.month)
    exprs = ", ".join(f"count(*) FILTER (WHERE NOT coalesce(({sql}), false))" for _, sql in rules)
    total, rejected, *counts = ctx.con.execute(
        f"SELECT count(*), count(_reject_reason), {exprs} FROM {ctx.table}"
    ).fetchone()
    out = []
    for (name, _), n in zip(rules, counts, strict=True):
        # Per-rule results are informational; the batch decision is the aggregate below.
        out.append(
            CheckResult(
                f"row_rule.{name}",
                "WARN",
                _ok(n > 0),
                observed=str(n),
                threshold="0",
                message=f"{n} of {total} rows break '{name}'",
            )
        )
    rate = rejected / total if total else 1.0
    limit = ctx.contract.max_rejected_row_rate
    out.append(
        CheckResult(
            "rejected_row_rate",
            "ERROR",
            _ok(rate > limit),
            observed=f"{rate:.4%} ({rejected} rows)",
            threshold=f"<= {limit:.2%}",
            message=f"{rejected} of {total} rows rejected ({rate:.3%}); limit {limit:.2%}",
        )
    )
    return out


# ---------- batch-level checks ----------


def check_row_count_band(ctx: AuditContext) -> list[CheckResult]:
    cfg = ctx.contract.checks.row_count_band
    (n,) = ctx.con.execute(f"SELECT count(*) FROM {ctx.table}").fetchone()
    # Baseline = the trailing N *published months before this one*. Using month order
    # (not publish order) keeps the decision the same no matter which order months run.
    prior = ctx.con.execute(
        """
        SELECT row_count FROM batches
        WHERE status = 'published' AND month < ?
        ORDER BY month DESC LIMIT ?
        """,
        [ctx.month, cfg.lookback_months],
    ).fetchall()
    if not prior:
        return [
            CheckResult(
                "row_count_band",
                cfg.severity,
                "pass",
                observed=str(n),
                threshold="n/a",
                message="no earlier published months; no baseline yet",
            )
        ]
    baseline = sum(r[0] for r in prior) / len(prior)
    ratio = n / baseline
    failed = abs(ratio - 1) > cfg.tolerance
    return [
        CheckResult(
            "row_count_band",
            cfg.severity,
            _ok(failed),
            observed=f"{n} rows ({ratio:.0%} of baseline)",
            threshold=f"{1 - cfg.tolerance:.0%}..{1 + cfg.tolerance:.0%} of {baseline:,.0f}",
            message=f"{n:,} rows vs trailing {len(prior)}-month average {baseline:,.0f} ({ratio - 1:+.1%})",
        )
    ]


def check_duplicate_rows(ctx: AuditContext) -> list[CheckResult]:
    cfg = ctx.contract.checks.duplicate_rows
    key = ", ".join(f'"{k}"' for k in cfg.key)
    total, distinct = ctx.con.execute(f"SELECT count(*), count(DISTINCT ({key})) FROM {ctx.table}").fetchone()
    dups = total - distinct
    rate = dups / total if total else 0.0
    return [
        CheckResult(
            "duplicate_rows",
            cfg.severity,
            _ok(rate > cfg.max_rate),
            observed=f"{rate:.4%} ({dups} rows)",
            threshold=f"<= {cfg.max_rate:.2%}",
            message=f"{dups:,} duplicate rows on the trip key",
        )
    ]


def check_duplicate_batch(ctx: AuditContext) -> list[CheckResult]:
    """Same bytes already published under a DIFFERENT month = someone sent the wrong file."""
    row = ctx.con.execute(
        "SELECT month FROM batches WHERE checksum = ? AND status = 'published' AND month <> ? LIMIT 1",
        [ctx.checksum, ctx.month],
    ).fetchone()
    return [
        CheckResult(
            "duplicate_batch",
            "ERROR",
            _ok(row is not None),
            observed=row[0] if row else "none",
            threshold="none",
            message=f"identical file already published as {row[0]}" if row else "file content is new",
        )
    ]


def check_freshness(ctx: AuditContext) -> list[CheckResult]:
    sev = ctx.contract.checks.freshness.severity
    (latest,) = ctx.con.execute("SELECT max(month) FROM batches WHERE status = 'published'").fetchone()
    if latest is None:
        return [CheckResult("freshness", sev, "pass", message="first month ever loaded")]
    expected = months.add(latest, 1)
    if ctx.month <= latest:
        return [
            CheckResult(
                "freshness",
                sev,
                "pass",
                observed=ctx.month,
                threshold=f"<= {expected}",
                message=f"re-run/correction of an already covered period (latest {latest})",
            )
        ]
    if ctx.month == expected:
        return [
            CheckResult("freshness", sev, "pass", observed=ctx.month, threshold=expected, message="next expected month")
        ]
    if ctx.mode == "backfill":
        return [
            CheckResult(
                "freshness",
                sev,
                "pass",
                observed=ctx.month,
                threshold=expected,
                message=f"gap after {latest} allowed in backfill mode",
            )
        ]
    return [
        CheckResult(
            "freshness",
            sev,
            "fail",
            observed=ctx.month,
            threshold=expected,
            message=f"expected {expected} next, got {ctx.month}; months would be missing",
        )
    ]


ALL_CHECKS = [
    check_schema,
    check_null_rates,
    check_row_rules,
    check_row_count_band,
    check_duplicate_rows,
    check_duplicate_batch,
    check_freshness,
]


def run_checks(ctx: AuditContext) -> list[CheckResult]:
    results: list[CheckResult] = []
    for check in ALL_CHECKS:
        results.extend(check(ctx))
    return results


def store_results(con: duckdb.DuckDBPyConnection, batch_id: int, results: list[CheckResult]) -> None:
    con.executemany(
        "INSERT INTO check_results VALUES (?, ?, ?, ?, ?, ?, ?)",
        [[batch_id, r.check_name, r.severity, r.status, r.observed, r.threshold, r.message] for r in results],
    )
