"""Vectorised cleaning / standardisation of raw booking rows.

Every function here works on whole columns (no row loops) and never drops
rows: values that cannot be interpreted become null so that validation can
quarantine the row with an explicit reason.
"""
from __future__ import annotations

import pandas as pd

from .config import Config


def normalise_whitespace(s: pd.Series) -> pd.Series:
    """Trim and collapse internal runs of whitespace ('Travel  Agent' -> 'Travel Agent')."""
    return s.str.strip().str.replace(r"\s+", " ", regex=True)


def parse_dates(s: pd.Series, formats: list[str]) -> pd.Series:
    """Parse a column of mixed-format dates, trying each format in order.

    Each format is applied to the whole column at once, so the cost is
    O(rows x formats), not a Python call per row.
    """
    s = normalise_whitespace(s)
    out = pd.Series(pd.NaT, index=s.index, dtype="datetime64[ns]")
    for fmt in formats:
        todo = out.isna() & s.notna()
        if not todo.any():
            break
        out.loc[todo] = pd.to_datetime(s[todo], format=fmt, errors="coerce")
    return out


def parse_numeric(s: pd.Series) -> pd.Series:
    """'12,000.00' -> 12000.0; anything unparseable -> NaN."""
    return pd.to_numeric(s.str.replace(r"[,\s]", "", regex=True), errors="coerce")


def standardise_currency(s: pd.Series, currency_map: dict[str, str]) -> pd.Series:
    """Map symbols / variants to ISO codes. Unknown codes are kept (upper-cased)
    so validation can report *which* code was unknown."""
    key = normalise_whitespace(s).str.upper()
    return key.map(currency_map).fillna(key)


def standardise_channel(s: pd.Series, channel_map: dict[str, str]) -> pd.Series:
    """'O.T.A', ' ota ', 'Travel  Agent' -> canonical label; unmapped -> null."""
    key = s.str.lower().str.replace(r"[^a-z]", "", regex=True)
    return key.map(channel_map)


def standardise_status(s: pd.Series, status_map: dict[str, str]) -> pd.Series:
    return normalise_whitespace(s).str.lower().map(status_map)


def clean_bookings(raw: pd.DataFrame, cfg: Config) -> pd.DataFrame:
    """Return a typed, standardised copy of `raw` (same index, same row count)."""
    out = pd.DataFrame(index=raw.index)
    out["booking_id"] = normalise_whitespace(raw["booking_id"]).str.upper()
    out["property_id"] = normalise_whitespace(raw["property_id"]).str.upper()
    out["check_in_date"] = parse_dates(raw["check_in"], cfg.stay_date_formats)
    out["check_out_date"] = parse_dates(raw["check_out"], cfg.stay_date_formats)
    out["nights"] = (out["check_out_date"] - out["check_in_date"]).dt.days.astype("Int64")
    out["num_guests"] = parse_numeric(raw["num_guests"])
    out["room_rate"] = parse_numeric(raw["room_rate"])
    out["currency"] = standardise_currency(raw["currency"], cfg.currency_map)
    out["revenue"] = parse_numeric(raw["revenue"])
    out["booking_channel"] = standardise_channel(raw["booking_channel"], cfg.channel_map)
    out["booking_status"] = standardise_status(raw["booking_status"], cfg.status_map)
    out["created_at"] = pd.to_datetime(normalise_whitespace(raw["created_at"]),
                                       format=cfg.timestamp_format, errors="coerce")
    out["updated_at"] = pd.to_datetime(normalise_whitespace(raw["updated_at"]),
                                       format=cfg.timestamp_format, errors="coerce")
    for col in ("_batch_id", "_batch_date", "_source_row"):
        out[col] = raw[col]
    return out
