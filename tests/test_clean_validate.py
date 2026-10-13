"""Cleaning and validation rules."""
import pandas as pd
import pytest

from flux_pipeline.clean import (clean_bookings, parse_dates, parse_numeric,
                                 standardise_channel, standardise_currency)
from flux_pipeline.enrich import attach_fx, attach_properties
from flux_pipeline.validate import validate

FORMATS = ["%Y-%m-%d", "%d/%m/%Y", "%d %b %Y"]


def test_mixed_date_formats_parse_day_first():
    s = pd.Series(["2025-05-04", "24/04/2025", "03/04/2025", "22 Jul 2025",
                   "1 Aug 2025", " 2025-12-01 ", "31/02/2025", "garbage", None])
    out = parse_dates(s, FORMATS)
    assert out[:6].tolist() == pd.to_datetime(
        ["2025-05-04", "2025-04-24", "2025-04-03", "2025-07-22",
         "2025-08-01", "2025-12-01"]).tolist()
    assert out[6:].isna().all()          # impossible date, junk and null -> NaT


def test_numeric_strips_thousands_separators():
    out = parse_numeric(pd.Series(["12,000.00", "4100", " 9 200 ", "ab", None, "-5"]))
    assert out[:3].tolist() == [12000.0, 4100.0, 9200.0]
    assert out[3:5].isna().all() and out[5] == -5.0


def test_currency_symbols_and_case_map_to_iso(cfg):
    raw = pd.Series(["€", "£", "$", "US$", "R", "eur", " gbp ", "ZAR", "XYZ"])
    assert standardise_currency(raw, cfg.currency_map).tolist() == [
        "EUR", "GBP", "USD", "USD", "ZAR", "EUR", "GBP", "ZAR", "XYZ"]


def test_channel_variants_collapse(cfg):
    raw = pd.Series(["O.T.A", "ota", "WEBSITE ", " Direct ", "Travel  Agent",
                     "travel agent", "fax"])
    assert standardise_channel(raw, cfg.channel_map).tolist()[:6] == [
        "OTA", "OTA", "Website", "Direct", "Travel Agent", "Travel Agent"]
    assert pd.isna(standardise_channel(raw, cfg.channel_map).iloc[6])


# --- validation ---------------------------------------------------------

def raw_row(**overrides):
    row = {"booking_id": "B1", "property_id": "P001", "check_in": "2025-08-01",
           "check_out": "2025-08-03", "num_guests": "2", "room_rate": "1000",
           "currency": "USD", "revenue": "2,000.00", "booking_channel": "direct ",
           "booking_status": "confirmed", "created_at": "2025-05-01 00:00:00",
           "updated_at": "2025-05-01 00:00:00", "_batch_id": "b1",
           "_batch_date": pd.Timestamp("2025-09-01"), "_source_row": 1}
    row.update(overrides)
    return row


@pytest.fixture
def props():
    return pd.DataFrame({"property_id": ["P001", "P009"], "property_name": ["A", "B"],
                         "country": ["South Africa"] * 2, "region": ["X", "Y"],
                         "room_count": [10, 5], "property_status": ["active", "renovation"]})


def run_validation(rows, cfg, fx, props):
    cleaned = clean_bookings(pd.DataFrame(rows), cfg)
    enriched = attach_fx(attach_properties(cleaned, props), fx)
    return validate(enriched, cfg, set(props["property_id"]))


def test_clean_row_passes(cfg, fx, props):
    out = run_validation([raw_row()], cfg, fx, props)
    assert out.loc[0, "_reject_reasons"] == ""
    assert out.loc[0, "revenue_zar"] == pytest.approx(2000 * 18.5)


@pytest.mark.parametrize("overrides,reason", [
    ({"revenue": "-12300"}, "non_positive_revenue"),
    ({"revenue": "0"}, "non_positive_revenue"),
    ({"revenue": None}, "missing_or_bad_revenue"),
    ({"check_out": "2025-07-25"}, "check_out_not_after_check_in"),
    ({"check_out": "2025-08-01"}, "check_out_not_after_check_in"),
    ({"property_id": "P099"}, "unknown_property"),
    ({"currency": "XYZ"}, "unknown_currency_or_no_fx_rate"),
    ({"num_guests": "99"}, "invalid_num_guests"),
    ({"num_guests": None}, "invalid_num_guests"),
    ({"updated_at": "2025-03-31 00:00:00"}, "updated_at_before_created_at"),
    ({"booking_channel": "carrier pigeon"}, "unknown_channel"),
    ({"check_in": "31/02/2025"}, "unparseable_check_in"),
])
def test_bad_rows_are_rejected_with_reason(cfg, fx, props, overrides, reason):
    out = run_validation([raw_row(**overrides)], cfg, fx, props)
    assert reason in out.loc[0, "_reject_reasons"].split(";")


def test_multiple_reasons_are_all_recorded(cfg, fx, props):
    out = run_validation([raw_row(revenue="0", check_out="2025-08-01")], cfg, fx, props)
    assert set(out.loc[0, "_reject_reasons"].split(";")) == {
        "non_positive_revenue", "check_out_not_after_check_in"}


def test_soft_rules_warn_but_do_not_reject(cfg, fx, props):
    out = run_validation([raw_row(property_id="P009", revenue="1500")], cfg, fx, props)
    assert out.loc[0, "_reject_reasons"] == ""
    assert set(out.loc[0, "_warnings"].split(";")) == {
        "revenue_not_rate_x_nights", "property_not_active"}


def test_created_after_batch_date_is_configurable(cfg, fx, props):
    row = raw_row(created_at="2025-09-20 00:00:00", updated_at="2025-09-20 00:00:00",
                  check_in="2025-10-01", check_out="2025-10-03")
    lenient = run_validation([row], cfg, fx, props)
    assert lenient.loc[0, "_reject_reasons"] == ""
    assert "created_after_batch_date" in lenient.loc[0, "_warnings"]

    from dataclasses import replace
    strict = run_validation([row], replace(cfg, reject_created_after_batch_date=True), fx, props)
    assert strict.loc[0, "_reject_reasons"] == "created_after_batch_date"
