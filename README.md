# Flux bookings pipeline: Data Engineer assessment

This repo is a re-runnable, incremental pipeline. It turns daily booking
extracts into a clean, analysis-ready table: one row per booking, revenue
converted to ZAR at the FX rate in effect when the booking was created. It also
includes the BigQuery SQL that models and reports on that table.

| Deliverable | Where |
|---|---|
| Pipeline (Part B) | `flux_pipeline/`, config in `config/pipeline.yaml` |
| Tests | `tests/` (48 pytest tests) |
| SQL (Part A) | `sql/flux_assessment.sql` (BigQuery dialect) |
| Output artefacts | `output/`: `analysis_ready_bookings.csv/.parquet`, `rejects.csv`, `dq_summary.csv`, `merge_audit.csv`, `stg_bookings.csv`, `sql_results/*.csv` |
| Part C write-up | `docs/PART_C_production.md` |

## How to run

Requires Python 3.10+.

```bash
python -m venv .venv
.venv/Scripts/activate            # Windows (Git Bash); on macOS/Linux: source .venv/bin/activate
pip install -r requirements.txt

python -m flux_pipeline                  # process any new batches in data/raw
python -m flux_pipeline --force          # reprocess every batch (shows idempotency)
python -m flux_pipeline --full-refresh   # delete state, rebuild from all batches
python scripts/run_sql_duckdb.py         # run Part A SQL locally, write output/sql_results/

pytest                                    # run the tests
```

If any data-quality check marked `FAIL` is hit, the pipeline exits with code 1.
That lets a scheduler stop the run before bad data reaches reporting.

## How it works

```
data/raw/*.csv ─► read as text ─► clean ─► enrich (FX, property) ─► validate ─┬─► rejects (with reasons)
                                                                               └─► merge into current state ─► analysis-ready + DQ summary + merge audit
```

| Module | Responsibility |
|---|---|
| `io.py` | Finds batches (batch date comes from the file name), reads every value as text, tags each row with lineage (`_batch_id`, `_batch_date`, `_source_row`), writes files atomically, keeps the checksum manifest |
| `clean.py` | Column-wise standardisation: three date formats, `12,000.00` → 12000, `€/£/$/US$/R` → ISO currency codes, channel variants (`O.T.A`, `Travel  Agent`, …), status, whitespace |
| `enrich.py` | Point-in-time FX lookup (`merge_asof` on the `created_at` date and currency), property attributes |
| `validate.py` | Hard rules send a row to quarantine (every reason is recorded, `;`-separated). Soft rules only add a warning |
| `merge.py` | Idempotent upsert into current state, plus per-batch counts |
| `model.py` | Analysis-ready table and the status flags |
| `dq.py` | Data-quality summary with a PASS/WARN/FAIL/INFO status per check |
| `pipeline.py` | Runs the steps in order: manifest skip, checkpointing, publishing outputs |

### Incremental merge and idempotency

For each `booking_id` we keep the version with the greatest **version key**
`(updated_at, _batch_date, _source_row)`:

- `updated_at` decides which version is newest, as the brief specifies.
- The batch date, then the row position in the file, only break exact
  `updated_at` ties. That makes the result deterministic. The tie case is
  flagged in DQ: B1021 has two batch-1 rows with the same timestamp and
  different revenue.
- "Keep the maximum" gives the same result however many times you apply it and
  in whatever order the batches arrive. So re-running, re-sent rows and
  out-of-order deltas all end in the same state:
  - a stale version is counted as `stale_ignored` and never applied;
  - an identical re-send is counted as `unchanged`.
- Tests prove this: re-running gives byte-identical outputs, `--force` gives 0
  inserts and 0 updates, and running batch 2 before batch 1 gives the same
  result as the normal order.
- In SQL the same rule is a `MERGE … WHEN MATCHED AND s.updated_at > t.updated_at`.

Other safeguards:
- **Checksum manifest:** a batch already processed is skipped unless its file
  has changed or `--force` is given.
- **Partition replace:** reprocessing a batch replaces its staging and reject
  rows rather than appending them.
- **Write order:** tables are written first and the manifest last, so a crash
  between them just means the batch is reprocessed, which is safe.

The merge audit (`output/merge_audit.csv`, one row per batch per run) satisfies
`rows_read = rows_rejected + rows_valid` and
`rows_valid = intra_batch_superseded + inserted + updated + unchanged + stale_ignored`.

### Status modelling

**All current bookings are kept, one row per booking, with explicit status flags.**

- **Cancelled bookings stay in the table.** A cancellation has to *override*
  the earlier "confirmed" version, and cancellation rate and lost revenue are
  metrics in their own right.
- **`revenue_zar`** is the gross booked value, whatever the status.
- **`recognised_revenue_zar`** is `revenue_zar` for **confirmed** bookings and
  0 otherwise.
- **Reporting "revenue" means recognised revenue.** Cancelled bookings are
  excluded because the money won't be earned. Pending bookings are excluded
  because it hasn't been earned yet and many never convert.
- **The SQL reporting view uses the same rule,** so analysts can't apply it
  inconsistently.

## Assumptions

- **Dates:** slash dates are **day-first** (`dd/mm/yyyy`). 34 values have a day
  above 12; none has a month above 12.
- **Currency:** a bare `$` means USD, the only dollar currency in `fx_rates`.
  `R` means ZAR.
- **FX:** the rate used is the one in effect on the **date** of `created_at`;
  the time of day is ignored. A booking with no applicable rate is rejected.
- **Revenue month:** revenue is reported by **stay month** (`check_in_date`).
- **Rejected rows:** rejected rows never enter the merge, so the last *valid*
  version stays current. Example: B1059's batch-2 row has `updated_at` before
  `created_at`.
- **Warnings rather than rejects:**
  - `created_at` after the batch file date (15 rows). Batch 2 also contains
    updates dated after its own file date, so the file date looks like a label,
    not a cut-off. Set `reject_created_after_batch_date: true` to quarantine
    these rows instead.
  - Revenue that doesn't equal `room_rate × nights`, since amendments restate
    revenue.
  - Bookings at P009, which is under renovation.
- **Guests:** a valid count is 1–12 (configurable).
- **Properties:** `S. Africa` is standardised to `South Africa`.

### Results on the supplied data

| Batch | Rows read | Rejected | Inserted | Updated | Unchanged (re-sent) |
|---|---|---|---|---|---|
| Batch 1 (2025-09-01) | 265 | 7 | 256 | – | – |
| Batch 2 (2025-09-02) | 40 | 1 | 19 | 15 | 5 |

- **Duplicates within batch 1:** 2 duplicate rows were collapsed.
- **Output:** 275 current bookings: 213 confirmed, 27 pending, 35 cancelled.
- **Rejects:** the 8 rejects are the planted bad rows: negative or zero
  revenue, inverted dates, missing revenue, 99 guests, orphan property `P099`,
  currency `XYZ`, and the stale B1059 row.

## Where AI was used

This project was built with **Claude Code (Anthropic) as an AI pair-programmer**.
It was used to:

- profile the raw files;
- draft the module structure, the pipeline code, the tests, the SQL and these
  docs;
- check that the BigQuery SQL runs, by transpiling it to DuckDB.

The design decisions were reviewed and agreed during development. Examples:
the version-key merge rule, the status policy, and making the
"created after batch date" rule a warning rather than a reject. Every result in
this README comes from actually running the pipeline, the tests and the SQL.
I can walk through and defend any part of the code.
