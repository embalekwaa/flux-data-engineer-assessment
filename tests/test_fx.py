"""Point-in-time FX join."""
import pandas as pd
import pytest

from flux_pipeline.enrich import attach_fx


def bookings(*rows):
    return pd.DataFrame([{"currency": c, "created_at": pd.Timestamp(t), "revenue": r}
                         for c, t, r in rows])


@pytest.mark.parametrize("created_at,expected_rate", [
    ("2024-12-31 23:59:59", 17.5),   # day before a new rate -> previous rate
    ("2025-01-01 00:00:00", 18.5),   # exactly on effective_from -> new rate
    ("2025-01-01 18:30:00", 18.5),   # time of day ignored: rate is by created_at *date*
    ("2025-06-30 12:00:00", 18.5),
    ("2025-07-01 00:00:00", 19.0),
    ("2026-03-01 00:00:00", 19.0),   # after the last rate -> latest effective rate
])
def test_rate_effective_on_created_at_date(fx, created_at, expected_rate):
    out = attach_fx(bookings(("USD", created_at, 100.0)), fx)
    assert out.loc[0, "fx_rate_to_zar"] == expected_rate
    assert out.loc[0, "revenue_zar"] == pytest.approx(100.0 * expected_rate)


def test_rates_are_matched_per_currency(fx):
    out = attach_fx(bookings(("EUR", "2025-08-01", 10.0), ("ZAR", "2025-08-01", 10.0),
                             ("USD", "2025-08-01", 10.0)), fx)
    assert out["fx_rate_to_zar"].tolist() == [20.1, 1.0, 19.0]


def test_no_applicable_rate_gives_null(fx):
    out = attach_fx(bookings(("EUR", "2024-06-01", 10.0),     # before first EUR rate
                             ("XYZ", "2025-06-01", 10.0)), fx)  # unknown currency
    assert out["fx_rate_to_zar"].isna().all() and out["revenue_zar"].isna().all()


def test_output_preserves_input_row_order_and_index(fx):
    df = bookings(("USD", "2025-08-01", 1.0), ("USD", "2024-02-01", 1.0),
                  ("USD", "2025-03-01", 1.0))
    df.index = [10, 5, 7]
    out = attach_fx(df, fx)
    assert list(out.index) == [10, 5, 7]
    assert out["fx_rate_to_zar"].tolist() == [19.0, 17.5, 18.5]
