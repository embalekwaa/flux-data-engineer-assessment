"""Data-quality summary: one row per check with a value and PASS/WARN/FAIL/INFO."""
from __future__ import annotations

import pandas as pd

from .config import Config

FAIL, WARN, PASS, INFO = "FAIL", "WARN", "PASS", "INFO"


def _row(check: str, scope: str, value: float, status: str, detail: str = "") -> dict:
    return {"check": check, "scope": scope, "value": value, "status": status, "detail": detail}


def _zero_or(status: str, value: int) -> str:
    return PASS if value == 0 else status


def build_dq_summary(analysis: pd.DataFrame, rejects: pd.DataFrame,
                     batch_stats: pd.DataFrame, cfg: Config) -> pd.DataFrame:
    """
    analysis    : analysis-ready table (current state, enriched)
    rejects     : all quarantined rows, with `reject_reasons` and `_batch_id`
    batch_stats : one row per processed batch with rows_read / rows_rejected /
                  merge counts from its most recent processing
    """
    rows = []

    # --- per batch: quarantine rate and merge behaviour -------------------
    for b in batch_stats.to_dict("records"):   # one iteration per *batch*, not per row
        rate = b["rows_rejected"] / b["rows_read"] if b["rows_read"] else 0.0
        status = (FAIL if rate > cfg.max_reject_rate_fail
                  else WARN if rate > cfg.max_reject_rate_warn else PASS)
        rows.append(_row("reject_rate", b["_batch_id"], round(rate, 4), status,
                         f"{b['rows_rejected']}/{b['rows_read']} rows quarantined "
                         f"(warn>{cfg.max_reject_rate_warn}, fail>{cfg.max_reject_rate_fail})"))
        rows.append(_row("same_updated_at_conflicts", b["_batch_id"],
                         b["same_updated_at_conflicts"],
                         _zero_or(WARN, b["same_updated_at_conflicts"]),
                         "versions with equal updated_at but different content; "
                         "resolved by batch date then file row"))

    # --- reject reasons (one row per reason) -------------------------------
    if not rejects.empty:
        reason_counts = (rejects["reject_reasons"].str.split(";").explode()
                         .value_counts().sort_index())
        for reason, n in reason_counts.items():
            rows.append(_row(f"rejected:{reason}", "all_batches", int(n), INFO))

    # --- assertions on the analysis-ready table ----------------------------
    dup = int(analysis["booking_id"].duplicated().sum())
    rows.append(_row("duplicate_current_booking_ids", "analysis_ready", dup,
                     _zero_or(FAIL, dup), "must be exactly one row per booking_id"))
    for col in ("revenue_zar", "fx_rate_to_zar", "property_name", "check_in_date"):
        n = int(analysis[col].isna().sum())
        rows.append(_row(f"null_{col}", "analysis_ready", n, _zero_or(FAIL, n)))
    n = int((analysis["revenue_zar"] <= 0).sum())
    rows.append(_row("non_positive_revenue_zar", "analysis_ready", n, _zero_or(FAIL, n)))
    n = int((analysis["check_out_date"] <= analysis["check_in_date"]).sum())
    rows.append(_row("invalid_date_ranges", "analysis_ready", n, _zero_or(FAIL, n)))

    warn_counts = (analysis["dq_warnings"].replace("", pd.NA).dropna()
                   .str.split(";").explode().value_counts().sort_index())
    for warning, n in warn_counts.items():
        rows.append(_row(f"warning:{warning}", "analysis_ready", int(n), INFO))

    # --- shape of the output (informational) -------------------------------
    rows.append(_row("row_count", "analysis_ready", len(analysis), INFO))
    for status, n in analysis["booking_status"].value_counts().sort_index().items():
        rows.append(_row(f"status:{status}", "analysis_ready", int(n), INFO))
    rows.append(_row("recognised_revenue_zar_total", "analysis_ready",
                     round(float(analysis["recognised_revenue_zar"].sum()), 2), INFO))
    return pd.DataFrame(rows)


def has_failures(summary: pd.DataFrame) -> bool:
    return bool((summary["status"] == FAIL).any())
