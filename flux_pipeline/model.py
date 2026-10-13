"""Build the analysis-ready booking table from the current state.

Status modelling decision
-------------------------
Every current booking is kept, whatever its status, with one row per
booking_id. Cancelled bookings are not deleted, because:
  * a cancellation has to override an earlier 'confirmed' version (deleting it
    would let the stale confirmed row resurface on a full rebuild), and
  * cancellation rate / lost revenue are themselves reporting metrics.

Status is made explicit through flags and two revenue measures:
  * revenue_zar            gross booked value at point-in-time FX, any status
  * recognised_revenue_zar revenue_zar for confirmed bookings, else 0
Pending bookings are not yet earned, so they count toward booked value but not
recognised revenue. Reporting on "revenue" means recognised revenue.
"""
from __future__ import annotations

import pandas as pd

from .config import Config
from .enrich import attach_fx, attach_properties
from .validate import reasons, warning_rules

ANALYSIS_COLUMNS = [
    "booking_id", "property_id", "property_name", "country", "region", "property_status",
    "booking_channel", "booking_status", "is_confirmed", "is_pending", "is_cancelled",
    "check_in_date", "check_out_date", "stay_month", "nights", "num_guests",
    "currency", "room_rate", "revenue", "fx_rate_to_zar", "fx_effective_from",
    "revenue_zar", "recognised_revenue_zar", "dq_warnings",
    "created_at", "updated_at", "_batch_id", "_source_row",
]


def build_analysis_ready(state: pd.DataFrame, fx: pd.DataFrame,
                         props: pd.DataFrame, cfg: Config) -> pd.DataFrame:
    df = attach_fx(attach_properties(state, props), fx)
    status = df["booking_status"]
    df["is_confirmed"] = status.eq("confirmed")
    df["is_pending"] = status.eq("pending")
    df["is_cancelled"] = status.eq("cancelled")
    df["recognised_revenue_zar"] = df["revenue_zar"].where(df["is_confirmed"], 0.0)
    df["stay_month"] = df["check_in_date"].dt.to_period("M").dt.to_timestamp()
    df["dq_warnings"] = reasons(warning_rules(df, cfg))
    return df[ANALYSIS_COLUMNS].sort_values("booking_id", ignore_index=True)
