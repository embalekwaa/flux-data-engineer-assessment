import shutil
from dataclasses import replace
from pathlib import Path

import pandas as pd
import pytest

from flux_pipeline.config import PROJECT_ROOT, load_config


@pytest.fixture
def cfg():
    return load_config()


@pytest.fixture
def tmp_cfg(tmp_path: Path, cfg):
    """Config pointing at a private copy of the raw data, so tests never touch real state."""
    raw = tmp_path / "raw"
    shutil.copytree(PROJECT_ROOT / "data" / "raw", raw)
    return replace(cfg, raw_dir=raw, properties_file=raw / "properties.csv",
                   fx_rates_file=raw / "fx_rates.csv",
                   state_dir=tmp_path / "state", output_dir=tmp_path / "output")


@pytest.fixture
def fx():
    return pd.DataFrame({
        "currency": ["USD", "USD", "USD", "EUR", "ZAR"],
        "rate_to_zar": [17.5, 18.5, 19.0, 20.1, 1.0],
        "effective_from": pd.to_datetime(
            ["2024-01-01", "2025-01-01", "2025-07-01", "2025-01-01", "2024-01-01"]),
    })


def booking(booking_id="B1", updated_at="2025-06-01", batch_date="2025-09-01",
            source_row=1, **overrides) -> dict:
    """A cleaned, valid booking version (the shape merge_batch consumes)."""
    row = {
        "booking_id": booking_id, "property_id": "P001",
        "check_in_date": pd.Timestamp("2025-08-01"), "check_out_date": pd.Timestamp("2025-08-03"),
        "nights": 2, "num_guests": 2, "room_rate": 1000.0, "currency": "USD",
        "revenue": 2000.0, "booking_channel": "Direct", "booking_status": "confirmed",
        "created_at": pd.Timestamp("2025-05-01"), "updated_at": pd.Timestamp(updated_at),
        "_batch_id": f"batch_{batch_date}", "_batch_date": pd.Timestamp(batch_date),
        "_source_row": source_row,
    }
    row.update(overrides)
    return row


def frame(*rows: dict) -> pd.DataFrame:
    return pd.DataFrame(list(rows))
