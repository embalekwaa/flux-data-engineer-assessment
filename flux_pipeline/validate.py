"""Row-level validation: hard rules quarantine a row, soft rules only flag it.

Each rule is a vectorised boolean mask (True = violation). A row can break
several rules; all reasons are recorded, separated by ';'.
"""
from __future__ import annotations

import pandas as pd

from .config import Config


def reasons(masks: pd.DataFrame) -> pd.Series:
    """Turn a frame of boolean rule masks into a ';'-joined reason string per row.

    `bool_frame.dot(names)` concatenates the names of the True columns, which
    keeps this set-based instead of looping over rows.
    """
    if masks.empty:
        return pd.Series("", index=masks.index)
    names = pd.Series([f"{c};" for c in masks.columns], index=masks.columns)
    return masks.fillna(False).astype(bool).dot(names).str.rstrip(";")


def reject_rules(df: pd.DataFrame, cfg: Config, known_properties: set[str]) -> pd.DataFrame:
    """Hard rules. `df` must already be cleaned and enriched with FX."""
    guests = df["num_guests"]
    rules = {
        "missing_booking_id": df["booking_id"].isna(),
        "missing_or_bad_updated_at": df["updated_at"].isna(),
        "missing_or_bad_created_at": df["created_at"].isna(),
        "updated_at_before_created_at": df["updated_at"] < df["created_at"],
        "unparseable_check_in": df["check_in_date"].isna(),
        "unparseable_check_out": df["check_out_date"].isna(),
        "check_out_not_after_check_in": df["nights"].fillna(1) <= 0,
        "stay_too_long": df["nights"].fillna(0) > cfg.max_nights,
        "missing_or_bad_revenue": df["revenue"].isna(),
        "non_positive_revenue": df["revenue"] <= 0,
        "missing_or_bad_room_rate": df["room_rate"].isna() | (df["room_rate"] <= 0),
        "invalid_num_guests": guests.isna() | (guests % 1 != 0)
                              | (guests < cfg.min_guests) | (guests > cfg.max_guests),
        "unknown_currency_or_no_fx_rate": df["currency"].notna() & df["fx_rate_to_zar"].isna(),
        "missing_currency": df["currency"].isna(),
        "unknown_channel": df["booking_channel"].isna(),
        "unknown_status": df["booking_status"].isna(),
        "unknown_property": ~df["property_id"].isin(known_properties),
    }
    if cfg.reject_created_after_batch_date:
        # A record can't be created after the day its extract was taken.
        rules["created_after_batch_date"] = (
            df["created_at"].dt.normalize() > df["_batch_date"])
    return pd.DataFrame(rules, index=df.index)


def warning_rules(df: pd.DataFrame, cfg: Config) -> pd.DataFrame:
    """Soft rules: kept in the output but counted in the DQ summary."""
    expected = df["room_rate"] * df["nights"]
    rules = pd.DataFrame({
        # Amendments legitimately restate revenue (discounts, surcharges), so a
        # mismatch is informative rather than an error.
        "revenue_not_rate_x_nights": df["revenue"].notna() & expected.notna()
                                     & ((df["revenue"] - expected).abs() > 0.01),
        "property_not_active": df["property_status"].notna()
                               & (df["property_status"] != "active"),
    }, index=df.index)
    if not cfg.reject_created_after_batch_date:
        # Default: the batch file date is treated as a label rather than a hard
        # cut-off (batch 2 also carries updated_at values after its file date).
        rules["created_after_batch_date"] = (
            df["created_at"].dt.normalize() > df["_batch_date"])
    return rules


def validate(df: pd.DataFrame, cfg: Config, known_properties: set[str]) -> pd.DataFrame:
    """Return `df` with `_reject_reasons` and `_warnings` columns ('' when none)."""
    out = df.copy()
    out["_reject_reasons"] = reasons(reject_rules(df, cfg, known_properties))
    out["_warnings"] = reasons(warning_rules(df, cfg))
    return out


def split_valid(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    bad = df["_reject_reasons"] != ""
    return df[~bad].copy(), df[bad].copy()
