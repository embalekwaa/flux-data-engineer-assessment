"""Load pipeline configuration from YAML into a typed object."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CONFIG = PROJECT_ROOT / "config" / "pipeline.yaml"


@dataclass(frozen=True)
class Config:
    raw_dir: Path
    batch_glob: str
    properties_file: Path
    fx_rates_file: Path
    state_dir: Path
    output_dir: Path
    encoding: str
    stay_date_formats: list[str]
    timestamp_format: str
    currency_map: dict[str, str]
    channel_map: dict[str, str]
    status_map: dict[str, str]
    country_map: dict[str, str]
    min_guests: int
    max_guests: int
    max_nights: int
    reject_created_after_batch_date: bool
    max_reject_rate_warn: float
    max_reject_rate_fail: float

    @classmethod
    def from_dict(cls, raw: dict[str, Any], root: Path = PROJECT_ROOT) -> "Config":
        def path(p: str) -> Path:
            p = Path(p)
            return p if p.is_absolute() else root / p

        paths, src, std = raw["paths"], raw["source"], raw["standardise"]
        val, dq = raw["validation"], raw["dq_thresholds"]
        return cls(
            raw_dir=path(paths["raw_dir"]),
            batch_glob=paths["batch_glob"],
            properties_file=path(paths["properties_file"]),
            fx_rates_file=path(paths["fx_rates_file"]),
            state_dir=path(paths["state_dir"]),
            output_dir=path(paths["output_dir"]),
            encoding=src["encoding"],
            stay_date_formats=list(src["stay_date_formats"]),
            timestamp_format=src["timestamp_format"],
            currency_map={str(k).strip().upper(): v for k, v in std["currency_map"].items()},
            channel_map=dict(std["channel_map"]),
            status_map=dict(std["status_map"]),
            country_map=dict(std["country_map"]),
            min_guests=int(val["min_guests"]),
            max_guests=int(val["max_guests"]),
            max_nights=int(val["max_nights"]),
            reject_created_after_batch_date=bool(val["reject_created_after_batch_date"]),
            max_reject_rate_warn=float(dq["max_reject_rate_warn"]),
            max_reject_rate_fail=float(dq["max_reject_rate_fail"]),
        )


def load_config(path: Path | str = DEFAULT_CONFIG, root: Path = PROJECT_ROOT) -> Config:
    with open(path, encoding="utf-8") as f:
        return Config.from_dict(yaml.safe_load(f), root=root)
