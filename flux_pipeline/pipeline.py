"""Pipeline steps: ingest -> clean/enrich/validate -> merge -> publish.

Each step is a plain function, so the CLI (`run`) and the Mage blocks in
`mage_flux/` call exactly the same code.
"""
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
LINEAGE = ["_batch_id", "_batch_date", "_source_row", "_file_sha256", "_file_name"]


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


def new_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


# --- step 1: ingest ---------------------------------------------------------

def load_new_batches(cfg: Config, force: bool = False) -> pd.DataFrame:
    """Raw rows (as text) of every batch not processed yet, oldest batch first.

    A batch is skipped when the manifest already holds the same file checksum,
    unless `force` is set.
    """
    manifest = io.load_manifest(cfg.state_dir)
    frames = []
    for batch in io.discover_batches(cfg):
        seen = manifest.get(batch.batch_id)
        if seen and seen["sha256"] == batch.sha256 and not force:
            log.info("Skipping %s: already processed (same checksum)", batch.batch_id)
            continue
        if seen and seen["sha256"] != batch.sha256:
            log.warning("%s changed since it was processed; reprocessing", batch.batch_id)
        frames.append(io.read_batch(batch, cfg.encoding)
                        .assign(_file_sha256=batch.sha256, _file_name=batch.path.name))
    if not frames:
        return pd.DataFrame(columns=io.BOOKING_COLUMNS + LINEAGE)
    return pd.concat(frames, ignore_index=True)


# --- step 2: clean, enrich, validate ------------------------------------------

def clean_and_validate(raw: pd.DataFrame, cfg: Config, fx: pd.DataFrame,
                       props: pd.DataFrame) -> pd.DataFrame:
    """Clean + enrich + validate every row. Nothing is dropped here: each row gets a
    `_reject_reasons` value ('' = valid). The original text is kept as raw_* columns
    so rejects can be reported exactly as they arrived."""
    if raw.empty:
        return raw
    cleaned = clean_bookings(raw, cfg)
    enriched = attach_fx(attach_properties(cleaned, props), fx)
    checked = validate(enriched, cfg, known_properties=set(props["property_id"]))
    checked["_file_sha256"] = raw["_file_sha256"]
    checked["_file_name"] = raw["_file_name"]
    return checked.join(raw[io.BOOKING_COLUMNS].add_prefix("raw_"))


def reject_rate(checked: pd.DataFrame) -> pd.Series:
    """Share of rows quarantined, per batch (used as a gate before merging)."""
    if checked.empty:
        return pd.Series(dtype=float)
    return checked["_reject_reasons"].ne("").groupby(checked["_batch_id"]).mean()


# --- step 3: merge ------------------------------------------------------------

def merge_and_save(checked: pd.DataFrame, cfg: Config, run_id: str) -> list[dict]:
    """Merge each batch (oldest first) into current state and checkpoint after each.

    Returns one merge-audit row per batch.
    """
    store = StateStore(cfg)
    current = store.load("current", empty_state())
    staging = store.load("staging", pd.DataFrame())
    rejects = store.load("rejects", pd.DataFrame())
    batches = store.load("batches", pd.DataFrame(columns=AUDIT_COLUMNS))
    manifest = io.load_manifest(cfg.state_dir)
    audit_rows = []
    if checked.empty:
        return audit_rows

    ordered = checked.sort_values(["_batch_date", "_batch_id", "_source_row"])
    for batch_id, rows in ordered.groupby("_batch_id", sort=False):
        valid, rejected = split_valid(rows)
        result = merge_batch(current, valid)
        current = result.state

        batch_rejects = (rejected[[f"raw_{c}" for c in io.BOOKING_COLUMNS]]
                         .rename(columns=lambda c: c.removeprefix("raw_"))
                         .assign(_batch_id=rejected["_batch_id"],
                                 _batch_date=rejected["_batch_date"],
                                 _source_row=rejected["_source_row"],
                                 reject_reasons=rejected["_reject_reasons"]))
        staging = replace_partition(staging, conform(valid), batch_id)
        rejects = replace_partition(rejects, batch_rejects, batch_id)

        first = rows.iloc[0]
        audit = {"run_id": run_id, "_batch_id": batch_id,
                 "batch_date": first["_batch_date"].date().isoformat(),
                 "file_sha256": first["_file_sha256"], "rows_read": len(rows),
                 "rows_rejected": len(batch_rejects), **result.counts}
        audit_rows.append(audit)
        batches = replace_partition(batches, pd.DataFrame([audit]), batch_id)
        log.info("%s: read=%d rejected=%d inserted=%d updated=%d unchanged=%d stale=%d",
                 batch_id, len(rows), len(batch_rejects), result.counts["inserted"],
                 result.counts["updated"], result.counts["unchanged"],
                 result.counts["stale_ignored"])

        # Checkpoint tables first, manifest last: a crash in between just means
        # the batch is reprocessed next time, which the merge makes harmless.
        for name, df in (("current", current), ("staging", staging),
                         ("rejects", rejects), ("batches", batches)):
            store.save(name, df)
        manifest[batch_id] = {"sha256": first["_file_sha256"], "run_id": run_id,
                              "file": first["_file_name"]}
        io.save_manifest(cfg.state_dir, manifest)
    return audit_rows


# --- step 4: publish ------------------------------------------------------------

def publish(cfg: Config, audit_rows: list[dict], fx: pd.DataFrame,
            props: pd.DataFrame) -> pd.DataFrame:
    """Rebuild the output artefacts from state (cheap and deterministic).

    Returns the DQ summary; callers decide what a FAIL means (exit code / error).
    """
    store = StateStore(cfg)
    current = store.load("current", empty_state())
    staging = store.load("staging", pd.DataFrame())
    rejects = store.load("rejects", pd.DataFrame())
    batches = store.load("batches", pd.DataFrame(columns=AUDIT_COLUMNS))

    out = cfg.output_dir
    analysis = build_analysis_ready(current, fx, props, cfg)
    dq = build_dq_summary(analysis, rejects, batches, cfg)

    io.write_atomic(analysis, out / "analysis_ready_bookings.csv")
    io.write_atomic(analysis, out / "analysis_ready_bookings.parquet")
    if not staging.empty:
        staging = staging.sort_values(["booking_id", "updated_at", "_batch_id"])
    io.write_atomic(staging, out / "stg_bookings.csv")
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
    return dq


# --- CLI entry point -------------------------------------------------------------

def run(cfg: Config, force: bool = False, full_refresh: bool = False) -> int:
    """Run all steps. Returns a process exit code (0 ok, 1 DQ failure)."""
    if full_refresh and cfg.state_dir.exists():
        log.warning("Full refresh: deleting state in %s", cfg.state_dir)
        shutil.rmtree(cfg.state_dir)

    fx, props = io.read_fx_rates(cfg), io.read_properties(cfg)
    raw = load_new_batches(cfg, force)
    checked = clean_and_validate(raw, cfg, fx, props)
    audit_rows = merge_and_save(checked, cfg, new_run_id())
    dq = publish(cfg, audit_rows, fx, props)
    return 1 if has_failures(dq) else 0
