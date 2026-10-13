"""Incremental merge of a validated batch into the current-state table.

Rule: for each booking_id, the current row is the version with the greatest
*version key* (updated_at, _batch_date, _source_row) ever seen.

* updated_at decides which version is newest (per the source contract).
* _batch_date / _source_row only break exact updated_at ties, deterministically:
  the later extract (then later line in the file) wins.

Because "keep the max" is idempotent, commutative and associative, re-running
a batch, re-sending rows, or applying batches out of order all converge on
the same state. Stale versions (older than what we hold) are ignored and
counted, never applied.
"""
from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

KEY = "booking_id"
VERSION_COLS = ["updated_at", "_batch_date", "_source_row"]
BUSINESS_COLUMNS = [
    "booking_id", "property_id", "check_in_date", "check_out_date", "nights",
    "num_guests", "room_rate", "currency", "revenue", "booking_channel",
    "booking_status", "created_at", "updated_at",
]
LINEAGE_COLUMNS = ["_batch_id", "_batch_date", "_source_row"]
STATE_COLUMNS = BUSINESS_COLUMNS + LINEAGE_COLUMNS

_DTYPES = {
    "booking_id": "str", "property_id": "str", "currency": "str",
    "booking_channel": "str", "booking_status": "str", "_batch_id": "str",
    "check_in_date": "datetime64[ns]", "check_out_date": "datetime64[ns]",
    "created_at": "datetime64[ns]", "updated_at": "datetime64[ns]",
    "_batch_date": "datetime64[ns]",
    "nights": "int64", "num_guests": "int64", "_source_row": "int64",
    "room_rate": "float64", "revenue": "float64",
}


def conform(df: pd.DataFrame) -> pd.DataFrame:
    """Project to the state schema with fixed dtypes (so hashes compare like-for-like)."""
    return df[STATE_COLUMNS].astype(_DTYPES)


def empty_state() -> pd.DataFrame:
    return conform(pd.DataFrame({c: pd.Series(dtype="object") for c in STATE_COLUMNS}))


def content_hash(df: pd.DataFrame) -> pd.Series:
    """Hash of the business columns: equal hash = identical re-send."""
    return pd.util.hash_pandas_object(df[BUSINESS_COLUMNS], index=False)


def latest_per_key(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """Collapse a batch to one row per booking_id (max version key).

    Returns the collapsed frame and how many rows were superseded.
    """
    ordered = df.sort_values([KEY] + VERSION_COLS, kind="mergesort")
    latest = ordered.drop_duplicates(KEY, keep="last")
    return latest, len(df) - len(latest)


def _is_newer(a: pd.DataFrame, b: pd.DataFrame, sa: str, sb: str) -> pd.Series:
    """Vectorised lexicographic compare of version keys: a{sa} > b{sb}."""
    newer = pd.Series(False, index=a.index)
    tie = pd.Series(True, index=a.index)
    for col in VERSION_COLS:
        x, y = a[col + sa], b[col + sb]
        newer |= tie & (x > y)
        tie &= x == y
    return newer


@dataclass
class MergeResult:
    state: pd.DataFrame
    counts: dict[str, int]
    actions: pd.DataFrame   # booking_id, action (insert/update/unchanged/stale_ignored)


def merge_batch(current: pd.DataFrame, batch: pd.DataFrame) -> MergeResult:
    """Merge validated, cleaned batch rows into `current` and report what happened."""
    current = conform(current)
    batch = conform(batch)
    incoming, superseded = latest_per_key(batch)
    # Versions of one booking sharing an updated_at but differing in content:
    # the tie-break picks one deterministically, but the source is ambiguous.
    intra_conflicts = int((batch.assign(_hash=content_hash(batch))
                           .groupby([KEY, "updated_at"])["_hash"].nunique() > 1).sum())

    cur = current.assign(_hash=content_hash(current))
    inc = incoming.assign(_hash=content_hash(incoming))
    j = inc.merge(cur[[KEY, "_hash"] + VERSION_COLS], on=KEY, how="left",
                  suffixes=("", "_cur"), indicator=True)

    is_new = j["_merge"] == "left_only"
    same = ~is_new & (j["_hash"] == j["_hash_cur"])
    newer = ~is_new & _is_newer(j, j, "", "_cur")

    action = pd.Series("stale_ignored", index=j.index)
    action[newer] = "update"
    action[same] = "unchanged"          # identical re-send, even if the lineage differs
    action[is_new] = "insert"
    same_ts_conflict = (~is_new & ~same
                        & (j["updated_at"] == j["updated_at_cur"]))

    apply = action.isin(["insert", "update"])
    winners = incoming[incoming[KEY].isin(j.loc[apply, KEY])]
    state = pd.concat([current[~current[KEY].isin(winners[KEY])], winners])
    state = conform(state).sort_values(KEY, ignore_index=True)

    counts = {
        "rows_valid": len(batch),
        "intra_batch_superseded": superseded,
        "inserted": int((action == "insert").sum()),
        "updated": int((action == "update").sum()),
        "unchanged": int((action == "unchanged").sum()),
        "stale_ignored": int((action == "stale_ignored").sum()),
        "same_updated_at_conflicts": intra_conflicts + int(same_ts_conflict.sum()),
    }
    actions = pd.DataFrame({KEY: j[KEY], "action": action})
    return MergeResult(state, counts, actions)
