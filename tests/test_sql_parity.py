"""The BigQuery SQL (run on DuckDB via sqlglot) must agree with the Python pipeline."""
import pandas as pd
import pytest

from flux_pipeline.pipeline import run
from scripts.run_sql_duckdb import run_all


@pytest.fixture
def sql_and_pipeline(tmp_cfg):
    run(tmp_cfg)
    con, results = run_all(tmp_cfg)
    analysis = pd.read_csv(tmp_cfg.output_dir / "analysis_ready_bookings.csv")
    return con, results, analysis


def test_current_versions_and_revenue_zar_match(sql_and_pipeline):
    con, _, analysis = sql_and_pipeline
    sql = con.execute("SELECT booking_id, booking_status, revenue_zar, recognised_revenue_zar "
                      "FROM flux.v_bookings_reporting ORDER BY booking_id").fetchdf()
    py = analysis[["booking_id", "booking_status", "revenue_zar", "recognised_revenue_zar"]]
    assert sql["booking_id"].tolist() == py["booking_id"].tolist()
    assert sql["booking_status"].tolist() == py["booking_status"].tolist()
    assert sql["revenue_zar"].astype(float).round(2).tolist() == py["revenue_zar"].round(2).tolist()


def test_mom_query_totals_recognised_revenue(sql_and_pipeline):
    _, results, analysis = sql_and_pipeline
    mom = results["a4_mom_revenue_growth_2025"]
    in_2025 = pd.to_datetime(analysis["check_in_date"]).dt.year == 2025
    assert len(mom) == 12
    assert mom["revenue_zar"].sum() == pytest.approx(
        analysis.loc[in_2025, "recognised_revenue_zar"].sum())


def test_merge_is_idempotent_in_sql(sql_and_pipeline):
    con, _, _ = sql_and_pipeline
    from scripts.run_sql_duckdb import SQL_FILE, blocks, to_duckdb
    merge = dict(blocks(SQL_FILE.read_text(encoding="utf-8")))["a3_merge_fct_bookings"]
    before = con.execute("SELECT * FROM flux.fct_bookings ORDER BY booking_id").fetchdf()
    con.execute(to_duckdb(merge))
    after = con.execute("SELECT * FROM flux.fct_bookings ORDER BY booking_id").fetchdf()
    pd.testing.assert_frame_equal(before, after)


def test_raw_dq_assertions_match_pipeline_rejects(sql_and_pipeline):
    _, results, _ = sql_and_pipeline
    dq = results["a6_dq_assertions_raw"].set_index("check_name")["failing"]
    assert dq["orphan_property_ids"] == 1
    assert dq["invalid_date_ranges"] == 2
    assert dq["non_positive_revenue"] == 2
    assert dq["no_applicable_fx_rate"] == 1
    assert dq["conflicting_current_versions"] == 1      # B1021 tie in batch 1
    assert dq["fct_duplicate_booking_ids"] == 0
