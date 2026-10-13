"""End-to-end runs over the real assessment files, in an isolated temp directory."""
import pandas as pd

from flux_pipeline.pipeline import run

OUTPUTS = ["analysis_ready_bookings.csv", "rejects.csv", "stg_bookings.csv", "dq_summary.csv"]


def read_outputs(cfg):
    return {name: (cfg.output_dir / name).read_bytes() for name in OUTPUTS}


def test_full_run_produces_expected_shape(tmp_cfg):
    assert run(tmp_cfg) == 0
    out = pd.read_csv(tmp_cfg.output_dir / "analysis_ready_bookings.csv")
    rejects = pd.read_csv(tmp_cfg.output_dir / "rejects.csv")
    audit = pd.read_csv(tmp_cfg.output_dir / "merge_audit.csv")

    assert out["booking_id"].is_unique
    assert out["revenue_zar"].gt(0).all() and out["fx_rate_to_zar"].notna().all()
    assert set(out["booking_status"]) <= {"confirmed", "pending", "cancelled"}
    assert set(out["booking_channel"]) <= {"Direct", "Website", "OTA", "Travel Agent"}
    assert set(out["currency"]) <= {"USD", "EUR", "GBP", "ZAR"}
    assert (out.loc[~out["is_confirmed"], "recognised_revenue_zar"] == 0).all()
    assert not set(rejects["booking_id"]) & {"B1001"}           # clean row not rejected
    assert {"B9002", "B9006", "B9007"} <= set(rejects["booking_id"])
    # Audit arithmetic: every valid row is accounted for exactly once.
    parts = ["intra_batch_superseded", "inserted", "updated", "unchanged", "stale_ignored"]
    assert (audit[parts].sum(axis=1) == audit["rows_valid"]).all()
    assert (audit["rows_valid"] + audit["rows_rejected"] == audit["rows_read"]).all()


def test_rerun_and_forced_reprocess_do_not_change_outputs(tmp_cfg):
    run(tmp_cfg)
    first = read_outputs(tmp_cfg)

    run(tmp_cfg)                         # no new batches -> skipped
    assert read_outputs(tmp_cfg) == first

    run(tmp_cfg, force=True)             # reprocess everything, including batch 2
    assert read_outputs(tmp_cfg) == first
    audit = pd.read_csv(tmp_cfg.output_dir / "merge_audit.csv")
    last_run = audit[audit["run_id"] == audit["run_id"].iloc[-1]]
    assert last_run["inserted"].sum() == 0 and last_run["updated"].sum() == 0


def test_batches_processed_out_of_order_converge(tmp_cfg):
    """Batch 2 arriving before batch 1 must give the same current state."""
    batch1 = next(tmp_cfg.raw_dir.glob("bookings_batch1_*.csv"))
    hidden = batch1.rename(batch1.with_suffix(".hold"))
    run(tmp_cfg)                         # batch 2 only
    hidden.rename(batch1)
    run(tmp_cfg)                         # then batch 1 (older versions must not win)
    out_of_order = pd.read_csv(tmp_cfg.output_dir / "analysis_ready_bookings.csv")

    run(tmp_cfg, full_refresh=True)      # canonical order
    in_order = pd.read_csv(tmp_cfg.output_dir / "analysis_ready_bookings.csv")
    business = [c for c in in_order.columns if not c.startswith("_")]
    pd.testing.assert_frame_equal(out_of_order[business], in_order[business])


def test_cancellation_in_delta_overrides_confirmed(tmp_cfg):
    run(tmp_cfg)
    out = pd.read_csv(tmp_cfg.output_dir / "analysis_ready_bookings.csv").set_index("booking_id")
    # B1009: confirmed on 2025-06-18, cancelled on 2025-07-06 (later updated_at)
    assert out.loc["B1009", "booking_status"] == "cancelled"
    assert out.loc["B1009", "recognised_revenue_zar"] == 0
