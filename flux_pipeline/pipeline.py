"""Orchestrates one pipeline run: ingest -> clean -> enrich -> validate -> merge -> publish."""
from __future__ import annotations

import logging
import shutil
from datetime import datetime, timezone

import pandas as pd

from . import io
from .clean import clean_bookings
from .config import Config
from .dq import build_dq_summary, has_failures
from .enrich import attach_fx, attach_properties
from .merge import conform, empty_state, merge_batch
from .model import build_analysis_ready
from .validate import split_valid, validate

log = logging.getLogger(__name__)

AUDIT_COLUMNS = [
    "run_id", "_batch_id", "batch_date", "file_sha256", "rows_read", "rows_rejected",
    "rows_valid", "intra_batch_superseded", "inserted", "updated", "unchanged",
    "stale_ignored", "same_updated_at_conflicts",
]


class StateStore:
    """Local stand-in for warehouse tables. Each table is a Parquet file in state_dir.

    current  : current-state bookings (the MERGE target)
    staging  : every valid cleaned version, partitioned by _batch_id (history)
    rejects  : quarantined raw rows, partitioned by _batch_id
    batches  : latest merge stats per batch
    """

    def __init__(self, cfg: Config):
        self.dir = cfg.state_dir

    def load(self, name: str, default: pd.DataFrame) -> pd.DataFrame:
        df = io.read_parquet_or_none(self.dir / f"{name}.parquet")
        return default if df is None else df

    def save(self, name: str, df: pd.DataFrame) -> None:
        io.write_atomic(df, self.dir / f"{name}.parquet")


def replace_partition(table: pd.DataFrame, new_rows: pd.DataFrame, batch_id: str) -> pd.DataFrame:
    """Drop then insert one batch's rows, so reprocessing a batch replaces, never appends."""
    kept = table[table["_batch_id"] != batch_id] if not table.empty else table
    parts = [p for p in (kept, new_rows) if not p.empty]
    return pd.concat(parts, ignore_index=True) if parts else new_rows


def process_batch(raw: pd.DataFrame, current: pd.DataFrame, cfg: Config,
                  fx: pd.DataFrame, props: pd.DataFrame):
    """Pure function over DataFrames (no I/O): returns new state + side tables."""
    cleaned = clean_bookings(raw, cfg)
    enriched = attach_fx(attach_properties(cleaned, props), fx)
    checked = validate(enriched, cfg, known_properties=set(props["property_id"]))
    valid, rejected = split_valid(checked)

    result = merge_batch(current, valid)
    rejects = raw.loc[rejected.index].assign(reject_reasons=rejected["_reject_reasons"])
    return result, conform(valid), rejects


def run(cfg: Config, force: bool = False, full_refresh: bool = False) -> int:
    """Run the pipeline. Returns a process exit code (0 ok, 1 DQ failure)."""
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    if full_refresh and cfg.state_dir.exists():
        log.warning("Full refresh: deleting state in %s", cfg.state_dir)
        shutil.rmtree(cfg.state_dir)

    store = StateStore(cfg)
    fx, props = io.read_fx_rates(cfg), io.read_properties(cfg)
    current = store.load("current", empty_state())
    staging = store.load("staging", pd.DataFrame())
    rejects = store.load("rejects", pd.DataFrame())
    batches = store.load("batches", pd.DataFrame(columns=AUDIT_COLUMNS))
    manifest = io.load_manifest(cfg.state_dir)
    audit_rows = []

    for batch in io.discover_batches(cfg):
        seen = manifest.get(batch.batch_id)
        if seen and seen["sha256"] == batch.sha256 and not force:
            log.info("Skipping %s: already processed (same checksum)", batch.batch_id)
            continue
        if seen and seen["sha256"] != batch.sha256:
            log.warning("%s changed since it was processed; reprocessing", batch.batch_id)

        raw = io.read_batch(batch, cfg.encoding)
        result, valid, batch_rejects = process_batch(raw, current, cfg, fx, props)
        current = result.state
        staging = replace_partition(staging, valid, batch.batch_id)
        rejects = replace_partition(rejects, batch_rejects, batch.batch_id)

        audit = {"run_id": run_id, "_batch_id": batch.batch_id,
                 "batch_date": batch.batch_date.date().isoformat(),
                 "file_sha256": batch.sha256, "rows_read": len(raw),
                 "rows_rejected": len(batch_rejects), **result.counts}
        audit_rows.append(audit)
        batches = replace_partition(batches, pd.DataFrame([audit]), batch.batch_id)
        log.info("%s: read=%d rejected=%d inserted=%d updated=%d unchanged=%d stale=%d",
                 batch.batch_id, len(raw), len(batch_rejects), result.counts["inserted"],
                 result.counts["updated"], result.counts["unchanged"],
                 result.counts["stale_ignored"])

        # Checkpoint tables first, manifest last: a crash in between just means
        # the batch is reprocessed next time, which the merge makes harmless.
        for name, df in (("current", current), ("staging", staging),
                         ("rejects", rejects), ("batches", batches)):
            store.save(name, df)
        manifest[batch.batch_id] = {"sha256": batch.sha256, "run_id": run_id,
                                    "file": batch.path.name}
        io.save_manifest(cfg.state_dir, manifest)

    return publish(cfg, current, staging, rejects, batches, fx, props, audit_rows)


def publish(cfg: Config, current, staging, rejects, batches, fx, props, audit_rows) -> int:
    """Rebuild the output artefacts from state (cheap and deterministic)."""
    out = cfg.output_dir
    analysis = build_analysis_ready(current, fx, props, cfg)
    dq = build_dq_summary(analysis, rejects, batches, cfg)

    io.write_atomic(analysis, out / "analysis_ready_bookings.csv")
    io.write_atomic(analysis, out / "analysis_ready_bookings.parquet")
    io.write_atomic(staging.sort_values(["booking_id", "updated_at", "_batch_id"]),
                    out / "stg_bookings.csv")
    if not rejects.empty:
        rejects = rejects.sort_values(["_batch_id", "_source_row"])
    io.write_atomic(rejects, out / "rejects.csv")
    io.write_atomic(dq, out / "dq_summary.csv")

    audit_path = out / "merge_audit.csv"   # append-only run log
    if audit_rows:
        new = pd.DataFrame(audit_rows, columns=AUDIT_COLUMNS)
        if audit_path.exists():
            new = pd.concat([pd.read_csv(audit_path), new], ignore_index=True)
        io.write_atomic(new, audit_path)
    else:
        log.info("No new batches to process; outputs rebuilt from existing state")

    for r in dq[dq["status"].isin(["WARN", "FAIL"])].itertuples(index=False):
        log.warning("DQ %s: %s [%s] = %s %s", r.status, r.check, r.scope, r.value, r.detail)
    log.info("Published %d current bookings to %s", len(analysis), out)
    return 1 if has_failures(dq) else 0
