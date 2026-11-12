# Defense pack

Study guide for interviews. Every claim points at code you can open.

## Problem and user
Monthly data files change shape or contain garbage. If a pipeline writes them straight into production
tables, analysts silently get wrong numbers. The user is the engineer who owns the feed. They want trusted
tables, a history of every run, and a plain-English reason for every rejected batch.

## Architecture in one breath
A file lands (checksum, batch registered). It is written to a typed staging table, audited against a YAML
contract, and then either published (one transaction replaces the month) or quarantined (warehouse
untouched; reasons saved; a human decides). Everything is plain Python plus DuckDB SQL. Airflow only calls
the functions.

## End-to-end data flow (with code)
| Step | What happens | Where |
|---|---|---|
| Fetch | download to `.part`, rename (atomic) | `fetch.py: fetch_month` |
| Land | sha256; skip if same month+checksum already published; hardlink into `landing/<month>/<hash>.parquet`; insert `batches` row (`landed`) | `land.py`, `pipeline.py: _land` |
| Write | read the footer (corrupt files fail here) → `CREATE TABLE stg_trips__ID AS SELECT TRY_CAST(...)`; missing optional columns become NULL; `_reject_reason` = first row rule broken | `stage.py: load_staging` |
| Audit | every check → `check_results`; status `audited` or `quarantined` | `checks.py`, `pipeline.py: _audit` |
| Publish | `BEGIN; DELETE month; INSERT valid; replace rejected_rows; rebuild dims; supersede old batch; mark published; COMMIT` | `publish.py: publish` |
| Quarantine | status + `reasons.json` with next steps; staging kept for inspection | `publish.py: quarantine` |
| Resolve | `--discard` (drop staging, free file) or `--publish --reason` (override, recorded) | `pipeline.py: resolve` |

## Key decisions and the alternatives
1. **Partition overwrite vs row upsert.** Overwrite the month. Trips have no natural key, so `MERGE` would
   need a hash key that breaks when a corrected row changes. The month file is the unit of delivery. (D4)
2. **Batch vs row rejection.** Both: bad rows go to `rejected_rows`, and more than 0.5% bad rows quarantines
   the batch. Nov 2024 had 1,251 bad rows (0.034%), which is normal noise. 3.5% bad means something
   systemic is wrong. (D3)
3. **DuckDB vs Postgres.** DuckDB is a single file with no server, has columnar speed (~3.6M rows audited
   and published in ~1.3 s), real transactions, and window functions. Postgres would win with many
   concurrent writers or an always-on service, which this project does not need.
4. **Airflow vs a plain scheduler.** Airflow adds retries (fetch retries 3× because TLC publishes late), a
   UI, per-task history, and `max_active_runs=1`. Cost: heavy install, so it lives in its own venv and all
   logic stays testable without it. Cron + `pipeline run` would also work. (D14)
5. **Thresholds vs meaning.** Contract v2 replaced a 30% null limit with the rule "missing passenger fields
   only for flex fares", after the real null rate climbed 10% → 23% in four months. (D11)

## Failure modes and what happens
| Failure | Result |
|---|---|
| Truncated or corrupt file | `file_readable` fails → quarantined; can never be overridden |
| Crash during download | `.part` file only; the real path never holds half a file |
| Crash during publish | transaction rolls back; batch marked `failed`; warehouse identical (tested) |
| Two runs at once | second one fails fast on the `fcntl` lock (tested); DuckDB also allows one writer |
| Same file sent twice | `skipped` (no-op) |
| Last month's file sent as this month | `duplicate_batch` + `pickup_in_month` fail → quarantined |
| Month skipped (gap) | `freshness` fails unless the run is a backfill |
| TLC not published yet | `FetchError` "not published yet (HTTP 403)"; Airflow retries |

## Tradeoffs I accepted
- One machine, one writer. Simple and correct, but it does not scale out.
- Staging copies of quarantined batches use disk until someone resolves them.
- The row-count band cannot judge the very first month.
- The first rule broken is the reason stored per row, not all of them. (Per-rule counts are still in
  `check_results`.)

## Hardest concepts, explained simply
- **Idempotent:** doing it twice gives the same result as doing it once. Here, publishing deletes the
  month and inserts it again, so repeats cannot double-count.
- **Atomic:** all or nothing. A database transaction means a crash halfway through leaves no trace.
- **Write-audit-publish:** never let consumers see data you haven't checked. Write it somewhere private,
  check it, then swap it in.
- **Order-independent fingerprint:** `count(*)` plus `sum(hash(row))`. Same rows in any physical order
  give the same number, which is how the tests prove two warehouses hold the same content.

---

## Likely interview questions

**1. What is write-audit-publish and why use it?**
Write new data to an isolated staging area, run quality checks on it there, and only promote it to the
tables people query if it passes. Consumers never see unchecked data, and a failure costs nothing but a
quarantined batch. Netflix popularized it with Iceberg branches. Here the "branch" is a staging table
`stg_trips__ID` (`stage.py`), and promotion is one transaction (`publish.py`).

**2. How do you make a pipeline idempotent? Show where.**
Three layers:
- **Same input → no-op.** The checksum check is in `land.py: already_published`.
- **Same month → replace, never append.** `publish.py` does `DELETE ... WHERE pickup_month = ?` then
  `INSERT`, inside one transaction.
- **Decisions don't depend on run order.** The row-count baseline uses months *before* this one
  (`checks.py: check_row_count_band`).

Proven by `test_forced_rerun_five_times_gives_identical_tables` and `make reproduce` (5/5).

**3. What happens if the process crashes during publish?**
Everything after `BEGIN` is rolled back. DuckDB's write-ahead log means even a killed process leaves the
last committed state. `test_crash_during_publish_leaves_warehouse_unchanged` raises right after the
`DELETE` and checks the fingerprints of `fact_trips` and `rejected_rows`. The batch is marked `failed`, and
a re-run simply publishes again.

**4. Why replace a whole partition instead of upserting rows?**
There's no reliable row key in taxi trips. A corrected file can remove rows (an upsert would leave the
stale ones behind). The delivery unit is a month. One `DELETE` plus `INSERT` is simpler to reason about and
naturally idempotent. At larger scale this becomes a partition swap (Iceberg/Delta `replaceWhere`, or
`INSERT OVERWRITE PARTITION`).

**5. How do you decide between quarantining and publishing with warnings?**
Ask: "would an analyst get a wrong answer?" Missing or retyped-to-text columns, a collapsed row count,
duplicates, more than 0.5% bad rows, or an unreadable file mean yes, so ERROR. A harmless extra column, or
numeric type drift that casts cleanly (int → double), means no, so WARN. Severity is set per check in the
YAML, so the owner can tune it without code changes.

**6. How would you handle late-arriving or corrected files?**
A corrected file for a published month is just a re-run. A new checksum means it lands, is audited
against the trailing months *before* it, and replaces the month atomically. The old batch becomes
`superseded`, so history is kept (`test_corrected_file_replaces_the_month_not_appends`). Late months use
`--backfill` / `pipeline backfill`, which allows gaps. Truly late *rows* (a January trip in the February
file) are rejected by `pickup_in_month` today. At scale I'd route them to the correct partition and
re-publish that month.

**7. How would this change at 100× the data size?**
At ~360M rows a month:
- DuckDB on one box still handles it, but I'd switch the warehouse to Parquet/Iceberg on object storage
  with a query engine (DuckDB, Trino, Spark).
- WAP becomes Iceberg branches or a staging snapshot plus an atomic metadata swap.
- Checks become incremental, or run on a sample plus exact aggregates.
- Partitioning goes to day level.
- The file lock becomes a catalog-level commit, or an orchestrator-level concurrency limit.

The contract, check semantics, and batch state machine would not change; that's the design that carries
over.

**8. How did you model the fact and dimension tables and why?**
A star schema. `fact_trips` is one row per trip at the grain of the source. Dimensions are zones (from TLC's
lookup), payment type, vendor, rate code (codes from the TLC data dictionary), and a generated `dim_date`.
Dimensions are rebuilt inside the publish transaction, so they always match the facts.
`fact_zones_exist_in_dim_zone` and `dimension_keys_unique` are SQL assertions. Analysts get `v_trips` with
names joined in.

**9. Show a window-function query you wrote and explain it.**
From `sql/models/02_views.sql`, `v_monthly_summary`:
```sql
SELECT *, LAG(trips) OVER w AS prev_trips,
       round(100.0 * (trips - LAG(trips) OVER w) / LAG(trips) OVER w, 2) AS trips_mom_pct
FROM m WINDOW w AS (ORDER BY pickup_month);
```
`LAG` looks one row back in month order, so each month sees the previous month's trips. The first month
has none, so it's NULL. Also `RANK() OVER (PARTITION BY pickup_month ORDER BY sum(total_amount) DESC)`
ranks zones within each month (ties share a rank). And in `v_daily_running_revenue`,
`sum(...) OVER (PARTITION BY month_start ORDER BY date_day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT
ROW)` gives a month-to-date total that restarts every month.

**10. What would you monitor in production, and what would you alert on?**
- **Alert (page):**
  - any quarantined batch (the Airflow `outcome` task fails on purpose)
  - a `failed` publish
  - a month not published N days after TLC's usual release
  - any failing SQL assertion (`pipeline verify`)
- **Watch (dashboard, from `v_batch_quality` + `check_results`):**
  - rejected-row % per month and per rule
  - null rates over time (this is how the flex-fare trend was found)
  - row-count ratio vs baseline
  - publish duration and rows/s
  - number of overrides (too many means the contract is wrong)
