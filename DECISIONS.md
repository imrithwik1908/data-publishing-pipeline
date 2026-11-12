# Decisions

One entry per important choice: what, why, alternatives.

## D1. Profile the real data before writing rules
**What:** Before writing the contract I profiled Jan 2025 (3,475,226 rows).
**Found:** 144,118 rows (4.1%) have a negative `fare_amount`; 540,149 rows (15.5%) have a null `passenger_count`.
The negative fares cluster in payment types 0/2/3/4 (flex fare, cash, no charge, dispute). Only 14 are card
payments. Every null `passenger_count` is a `payment_type = 0` (flex fare) trip.
**Why it matters:** A naive "fare >= 0, reject the batch above 0.5% bad rows" rule would quarantine *every real
month*. Rules have to encode business meaning (a refunded dispute is valid data), not just ranges.
**Decision:** row rule `card_fare_non_negative` (`payment_type <> 1 OR fare_amount >= 0`); null-rate limit of
30% on flex-fare-affected columns.

## D2. Real schema drift observed (Nov 2024 to Jan 2025)
- `cbd_congestion_fee` (DOUBLE) appears from 2025-01. In the contract it is `required: false`; earlier months
  get NULL. Verified: 2024-11/12 published with 0 non-null values, 2025-01 with 3,474,944.
- Nov 2024, Dec 2024 and Jan 2025 have *identical* types otherwise (`passenger_count` int64, `RatecodeID` int64).
  The int-vs-double drift reported for older years did not show up in these files. The pipeline still handles it:
  same-family numeric changes are cast and raise a WARN (`schema.retyped_columns`), which a test covers.
- `Airport_fee` is capitalized differently across TLC years; the contract matches names case-insensitively and
  supports `aliases`.

## D3. Row-level vs batch-level rejection
**What:** Rows that break a row rule go to `rejected_rows` with a reason. If more than 0.5% of a batch's rows are
rejected, the whole batch is quarantined.
**Why:** Real files always contain a few hundred bad rows (e.g. 1,251 in Nov 2024, 0.034%). Rejecting the
batch for those would block every month. A large share of bad rows means something systemic is wrong
(a wrong-month file, a broken upstream export), and silently dropping 4% of revenue is worse than waiting.
**Alternatives:** reject the whole batch on any bad row (too strict on real data); drop bad rows with no cap
(hides systemic failures).

## D4. Replace the whole month partition instead of upserting rows
**What:** Publish = `DELETE` the month + `INSERT` the audited rows, in one transaction.
**Why:** Trip records have no stable primary key, so a row-level upsert has nothing reliable to match on. A
monthly file is the unit of delivery, so the month is the unit of replacement. Re-running gives the same result
every time (idempotent), and a corrected re-delivery replaces the month instead of adding to it.
**Alternative:** `MERGE` on a synthetic key (hash of columns). This breaks when a corrected row changes a hashed
value, and leaves the old version behind.

## D5. Atomic publish with a single DuckDB transaction
All writes (fact delete/insert, rejected rows, batch status) happen between `BEGIN` and `COMMIT`. Any exception
causes a `ROLLBACK`. A test injects a failure right after the `DELETE` and proves both tables keep the same
fingerprint.

## D6. Row-count baseline uses month order, not publish order
The baseline is the average of the 3 published months *before* the batch's month. If it were the 3 most
recently published batches, a backfill run in a different order could make different decisions. A test runs
4 months in 3 different orders and gets identical table fingerprints.

## D7. Duplicate delivery handling
- Same bytes (sha256), same month, already published: **skip** (no-op). `--force` re-publishes.
- Same bytes under a *different* month: **quarantine** (`duplicate_batch`). Someone sent the wrong file.
- Every row repeated inside a file: **quarantine** (`duplicate_rows` above 0.1%).

## D8. Run lock
`fcntl.flock` on `data/warehouse.lock`, non-blocking, so a second run fails fast with a clear error instead of
queueing. DuckDB already allows only one writing process per file. The lock makes that rule explicit and
covers the landing/quarantine files too. Limitation: works on one machine only.

## D9. Landing is content-addressed and hard-linked
Landed files are stored at `landing/<month>/<sha256 prefix>.parquet`. A hardlink is used when possible, so
landing costs no extra disk space (908 MB down to 620 MB for 3 months).

## D10. Content fingerprint for "byte-for-byte equivalent in content"
`count(*)` + `sum(hash(row))`. This does not depend on physical row order, so it shows that the content is the
same even if DuckDB stores the rows in a different order.

## D11. Contract v2: explain missing fields with a rule, not a percentage
**What:** `make reproduce` showed null `passenger_count` rising every month: 10.3%, 8.9%, 15.5%, 22.6%
(Nov 2024 to Feb 2025). Every one of those rows is a flex-fare trip (`payment_type = 0`). A fixed 30% null
limit would soon have quarantined good data.
**Decision:** a row rule `missing_fields_only_for_flex_fare`. Null-rate limits were loosened to 60%, as a gross
sanity net only. The contract version went to 2, and every batch records the contract version it was checked
with.
**Lesson:** thresholds that describe *today's* data rot; rules that describe *meaning* do not.

## D12. Steps are separate functions, coordinated through the `batches` table
`land_batch`, `audit_batch` and `publish_batch` each read and advance the batch status
(`landed -> audited -> published`). That lets Airflow run them as separate tasks. `publish` refuses any batch
that is not `audited`, so nothing unaudited can reach the warehouse, even when called by hand.

## D13. Humans resolve quarantines explicitly
`pipeline resolve ID --discard`, or `--publish --reason "..."`. An override requires a reason, which is stored
on the batch and shown in the report. An unreadable file can never be overridden. Discard frees the staging
table and landed file.
**Alternative:** auto-retry or auto-publish after N days. Rejected: the whole point is that a person decides.

## D14. Airflow 3.4 in its own virtualenv
Airflow pulls in about 370 MB and pins many libraries. It lives in `.venv-airflow`, installed with Airflow's
official constraints file. The DAG holds no logic: each task calls one `dpp` function. CI does not install
Airflow. `make airflow-test MONTH=2025-02` ran all 6 tasks successfully. The DAG has `max_active_runs=1`, on
top of the file lock.
**Alternative:** cron + `pipeline run`. That would work, but you lose retries, a UI, and run history per task.

## D15. Money stored as DOUBLE
The source files store money as DOUBLE, and the warehouse keeps it that way so values survive a round trip
exactly. Views round sums to cents. In a real finance warehouse I would use DECIMAL(12,2) and say so in the
contract.

## D16. Loose dependency floors
`uv add` writes floors like `typer>=0.29`, which conflicted with Airflow's constraints. Floors are now the
oldest versions the code needs (`duckdb>=1.1`, etc.). `uv.lock` still pins exact versions for reproducible
installs.
