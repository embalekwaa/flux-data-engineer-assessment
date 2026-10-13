"""Enrichment: point-in-time FX to ZAR and property attributes."""
from __future__ import annotations

import pandas as pd

PROPERTY_COLUMNS = ["property_name", "country", "region", "room_count", "property_status"]


def attach_fx(df: pd.DataFrame, fx: pd.DataFrame) -> pd.DataFrame:
    """Add the FX rate effective on each booking's created_at *date*.

    For each row we take the rate with the greatest effective_from that is
    <= created_at's date, for the same currency (an as-of join, the pandas
    equivalent of the SQL range join in sql/flux_assessment.sql). Rows with no
    applicable rate (unknown currency, or created before the first rate)
    get null rate / revenue_zar.
    """
    out = df.copy()
    out["_fx_date"] = out["created_at"].dt.normalize().astype("datetime64[ns]")
    out["_row"] = range(len(out))
    joinable = out["_fx_date"].notna() & out["currency"].notna()

    left = out.loc[joinable, ["_row", "_fx_date", "currency"]].sort_values("_fx_date")
    right = (fx.rename(columns={"effective_from": "fx_effective_from"})
               .astype({"fx_effective_from": "datetime64[ns]"})
               .sort_values("fx_effective_from"))
    matched = pd.merge_asof(
        left, right[["currency", "fx_effective_from", "rate_to_zar"]],
        left_on="_fx_date", right_on="fx_effective_from", by="currency",
        direction="backward", allow_exact_matches=True,
    ).set_index("_row")

    out["fx_rate_to_zar"] = out["_row"].map(matched["rate_to_zar"])
    out["fx_effective_from"] = out["_row"].map(matched["fx_effective_from"])
    out["revenue_zar"] = (out["revenue"] * out["fx_rate_to_zar"]).round(2)
    return out.drop(columns=["_fx_date", "_row"])


def attach_properties(df: pd.DataFrame, props: pd.DataFrame) -> pd.DataFrame:
    """Left-join property attributes; orphans get null attributes (validation flags them)."""
    lookup = props.set_index("property_id")[PROPERTY_COLUMNS]
    out = df.copy()
    for col in PROPERTY_COLUMNS:
        out[col] = out["property_id"].map(lookup[col])
    return out
