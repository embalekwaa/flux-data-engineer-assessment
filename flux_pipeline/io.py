"""Reading source files and writing outputs / state."""
from __future__ import annotations

import hashlib
import json
import os
import re
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from .config import Config

BOOKING_COLUMNS = [
    "booking_id", "property_id", "check_in", "check_out", "num_guests", "room_rate",
    "currency", "revenue", "booking_channel", "booking_status", "created_at", "updated_at",
]
_BATCH_DATE_RE = re.compile(r"(\d{4}-\d{2}-\d{2})")


@dataclass(frozen=True)
class BatchFile:
    path: Path
    batch_id: str          # file stem, e.g. bookings_batch2_2025-09-02
    batch_date: pd.Timestamp
    sha256: str


def discover_batches(cfg: Config) -> list[BatchFile]:
    """Find batch files and return them ordered by batch date."""
    batches = []
    for p in cfg.raw_dir.glob(cfg.batch_glob):
        m = _BATCH_DATE_RE.search(p.stem)
        if not m:
            raise ValueError(f"Cannot parse batch date from file name: {p.name}")
        batches.append(BatchFile(p, p.stem, pd.Timestamp(m.group(1)), file_sha256(p)))
    return sorted(batches, key=lambda b: (b.batch_date, b.batch_id))


def file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read_batch(batch: BatchFile, encoding: str = "utf-8") -> pd.DataFrame:
    """Read a batch as raw strings and tag every row with its lineage.

    Everything is read as text so that cleaning, not the CSV parser, decides
    how '12,000.00' or ' 2025-07-01' are interpreted.
    """
    df = pd.read_csv(batch.path, dtype=str, keep_default_na=False, na_values=[""],
                     encoding=encoding)
    missing = set(BOOKING_COLUMNS) - set(df.columns)
    if missing:
        raise ValueError(f"{batch.path.name} is missing columns: {sorted(missing)}")
    df = df[BOOKING_COLUMNS].copy()
    df["_batch_id"] = batch.batch_id
    df["_batch_date"] = batch.batch_date
    df["_source_row"] = range(1, len(df) + 1)   # 1-based data row, header excluded
    return df


def read_properties(cfg: Config) -> pd.DataFrame:
    props = pd.read_csv(cfg.properties_file, dtype=str, encoding=cfg.encoding)
    props = props.apply(lambda s: s.str.strip())
    props["country"] = props["country"].replace(cfg.country_map)
    props["room_count"] = pd.to_numeric(props["room_count"], errors="coerce").astype("Int64")
    props["property_status"] = props["property_status"].str.lower()
    if props["property_id"].duplicated().any():
        raise ValueError("properties.csv has duplicate property_id values")
    return props


def read_fx_rates(cfg: Config) -> pd.DataFrame:
    fx = pd.read_csv(cfg.fx_rates_file, dtype=str, encoding=cfg.encoding)
    fx["currency"] = fx["currency"].str.strip().str.upper()
    fx["rate_to_zar"] = pd.to_numeric(fx["rate_to_zar"], errors="raise")
    fx["effective_from"] = pd.to_datetime(fx["effective_from"], format="%Y-%m-%d")
    if fx.duplicated(["currency", "effective_from"]).any():
        raise ValueError("fx_rates.csv has more than one rate per currency/effective_from")
    return fx.sort_values(["currency", "effective_from"], ignore_index=True)


# --- state + outputs ------------------------------------------------------

def write_atomic(df: pd.DataFrame, path: Path) -> None:
    """Write CSV/Parquet via a temp file + rename so a crash never leaves a half file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    if path.suffix == ".parquet":
        df.to_parquet(tmp, index=False)
    else:
        df.to_csv(tmp, index=False, date_format="%Y-%m-%d %H:%M:%S")
    os.replace(tmp, path)


def read_parquet_or_none(path: Path) -> pd.DataFrame | None:
    return pd.read_parquet(path) if path.exists() else None


def load_manifest(state_dir: Path) -> dict[str, dict]:
    path = state_dir / "manifest.json"
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}


def save_manifest(state_dir: Path, manifest: dict[str, dict]) -> None:
    state_dir.mkdir(parents=True, exist_ok=True)
    tmp = state_dir / "manifest.json.tmp"
    tmp.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(tmp, state_dir / "manifest.json")
