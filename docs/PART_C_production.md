# Part C: Running this for real on GCP / BigQuery

**1. Incremental loading, partitioning and cost.**
Each daily extract lands unchanged in GCS, then gets loaded into an append-only
`raw_bookings` table (all STRING columns, partitioned by `_batch_date`) so
nothing is lost before validation. The cleaning and validation rules from this
repo (or a dbt model that mirrors them) write `stg_bookings` and
`rejects`. `stg_bookings` gets a partition overwrite per batch, which is what
`replace_partition` does locally, so reprocessing a batch replaces it rather
than appending. Then one `MERGE` into `fct_bookings` (see
`sql/flux_assessment.sql`, A3):
- Source is the batch deduplicated with `QUALIFY ROW_NUMBER()`.
- An update applies only `WHEN MATCHED AND s.updated_at > t.updated_at`.

That makes the load idempotent and safe against stale rows that arrive out of
order. `fct_bookings` is partitioned by `DATE_TRUNC(check_in_date, MONTH)` and
clustered by `property_id, booking_channel, booking_status`. Those match the
monthly, per-property and per-channel reports, and the partitions stay large
enough to be worthwhile. With tens of millions of rows the cost levers are:
- add a `check_in_date BETWEEN <batch min> AND <batch max>` predicate to the
  MERGE target so it prunes partitions rather than scanning the whole table;
- set `require_partition_filter` on raw/staging;
- select only the columns needed (BigQuery bills by bytes scanned per column);
- pre-aggregate monthly property/channel tables (incremental, or a
  materialized view) so dashboards don't scan the fact table;
- set table expiry on staging;
- watch `INFORMATION_SCHEMA.JOBS` for expensive queries, and set per-user or
  project byte quotas.

FX is a tiny dimension, so the range join is broadcast and adds no real cost.

**2. Orchestration and monitoring.**
Cloud Composer (Airflow), or Cloud Workflows for something this small, triggered
when a file arrives in GCS, with a daily schedule as fallback. The DAG is
`land → validate → stage → DQ gate → MERGE → post-load assertions → refresh
aggregates`. Each task is retryable because every step is idempotent.

The DQ gate runs assertions like A6 and the pipeline's `dq_summary`:
- a reject rate above threshold, duplicate current ids, null FX, or orphan
  properties **stops the run before the MERGE**, so bad data never reaches
  reporting;
- warnings go to Slack or email.

Health metrics come from the merge audit and go to Cloud Monitoring:
- rows read, rejected, inserted, updated, unchanged and stale per batch;
- freshness (`max(updated_at)` and the last successful batch);
- alerts on a missing batch, volume anomalies (for example ±3σ against the
  trailing 30 days), or any jump in rejects.

Rejects stay queryable in a table so the source owner can fix them and re-send.

**3. Maintainability.**
- **Version control:** Git with PRs and review.
- **CI (GitHub Actions or Cloud Build):** lint, then `pytest` (unit tests plus
  the SQL-parity test against DuckDB), then a BigQuery `--dry_run` for
  syntax/cost on every SQL change, then deploy to a dev dataset and run
  assertions there, then promote to prod.
- **Infrastructure as code:** Terraform for datasets, tables, IAM and the
  Composer environment.
- **Secrets:** Workload Identity or service accounts, never keys in the repo.
- **Moving to dbt:** at a larger scale I'd move the SQL into dbt. Models,
  tests, docs and lineage come out of the same code, with source freshness
  checks and exposures for the dashboards.
- **Documentation:** a README per pipeline plus an SOP runbook covering
  backfill (`--full-refresh` or reprocessing a date range), how to reprocess a
  rejected batch, how to add a currency or FX rate, and what to do when the
  DQ gate fails.
- **Lineage:** every row carries `_batch_id`, so you can trace it from the
  report to the source file. Dataplex/Data Catalog shows lineage at the table
  level.

**4. Assumptions and trade-offs (2-3 hour limit).**
- **`updated_at` decides which version is newest.** Equal timestamps with
  different content (1 case: B1021) resolve to the later extract, then the
  later file row, and are flagged in DQ.
- **FX is by `created_at` date, as the brief says.** A bare `$` is taken as
  USD.
- **Slash dates are day-first.** 34 have a day above 12, and none has a month
  above 12.
- **A row that fails validation is quarantined, and the last valid version
  stays current.** Example: B1059's batch-2 row has `updated_at <
  created_at`. The trade-off is that a bad *newer* row can't overwrite good
  data, but it also can't apply a real change until it's fixed and re-sent.
- **`created_at` after the batch file date is a warning, not a reject.** It
  affects 15 current rows, and batch 2 has updates dated after its own file
  date, so the date looks like a label. This is configurable.
- **Revenue that doesn't equal `room_rate × nights` is a warning.** Amendments
  restate revenue.
- **Property P009 is under renovation but its bookings stay in.** They're
  flagged.
- **Revenue is reported by stay month and covers confirmed bookings only.**
- **Local state is Parquet rather than BigQuery,** and the pipeline is pandas.
  Every transform is column-wise, so it ports directly to BigQuery SQL or
  Spark. At warehouse scale the merge should happen in BigQuery (the MERGE
  above), not in Python memory.
- **With more time:** schema contracts on landing (pandera or dbt contracts),
  a dbt project with sources and tests, SCD2 history of booking versions for
  as-was reporting, and property-dimension history.

*RAG / AI enrichment (optional).* Any AI step should run as a separate,
traceable enrichment layer, never inside the core facts:
- Use an LLM (Vertex AI Gemini via `ML.GENERATE_TEXT` in BigQuery) only where
  rules fail, for example mapping new free-text channel or currency values to
  canonical codes. Its suggestions go to a review table, and a reviewer adds
  the approved ones to `config/pipeline.yaml`.
- For RAG over guest reviews or property descriptions, store embeddings in
  BigQuery (`ML.GENERATE_EMBEDDING` + `VECTOR_SEARCH`) keyed by `property_id`,
  so answers can be joined back to governed revenue facts.
- Log model version and prompt with every output so results are reproducible
  and can be audited.
