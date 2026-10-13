"""Merge / idempotency: the highest-weighted behaviour, so the most tests."""
import pandas as pd
import pytest

from flux_pipeline.merge import BUSINESS_COLUMNS, empty_state, merge_batch
from tests.conftest import booking, frame


def apply(*batches: pd.DataFrame) -> pd.DataFrame:
    state = empty_state()
    for b in batches:
        state = merge_batch(state, b).state
    return state


def business(state: pd.DataFrame) -> pd.DataFrame:
    return state[BUSINESS_COLUMNS].sort_values("booking_id", ignore_index=True)


def test_new_bookings_are_inserted():
    result = merge_batch(empty_state(), frame(booking("B1"), booking("B2")))
    assert result.counts["inserted"] == 2
    assert list(result.state["booking_id"]) == ["B1", "B2"]


def test_newer_update_replaces_current_version():
    state = apply(frame(booking("B1", revenue=2000.0)))
    result = merge_batch(state, frame(booking("B1", updated_at="2025-06-05",
                                              batch_date="2025-09-02", revenue=1800.0,
                                              booking_status="cancelled")))
    assert result.counts["updated"] == 1
    row = result.state.iloc[0]
    assert row["revenue"] == 1800.0 and row["booking_status"] == "cancelled"


def test_stale_update_arriving_later_does_not_overwrite_newer_value():
    state = apply(frame(booking("B1", updated_at="2025-06-10", booking_status="cancelled")))
    # Older version (by updated_at) arrives in a *later* batch.
    late = frame(booking("B1", updated_at="2025-06-01", batch_date="2025-09-02",
                         booking_status="confirmed"))
    result = merge_batch(state, late)
    assert result.counts["stale_ignored"] == 1
    assert result.state.iloc[0]["booking_status"] == "cancelled"


def test_identical_resend_is_unchanged():
    b = frame(booking("B1"))
    state = apply(b)
    resent = frame(booking("B1", batch_date="2025-09-02", source_row=7))  # same content
    result = merge_batch(state, resent)
    assert result.counts == {**result.counts, "inserted": 0, "updated": 0, "unchanged": 1}
    pd.testing.assert_frame_equal(result.state, state)


def test_reprocessing_same_batch_is_idempotent():
    b1 = frame(booking("B1"), booking("B2"))
    b2 = frame(booking("B2", updated_at="2025-07-01", batch_date="2025-09-02", revenue=999.0),
               booking("B3", batch_date="2025-09-02"))
    once = apply(b1, b2)
    again = merge_batch(once, b2)
    pd.testing.assert_frame_equal(again.state, once)
    assert again.counts["inserted"] == again.counts["updated"] == 0
    assert again.counts["unchanged"] == 2


def test_batch_order_does_not_change_business_result():
    b1 = frame(booking("B1"), booking("B2", revenue=500.0))
    b2 = frame(booking("B1", updated_at="2025-07-01", batch_date="2025-09-02", revenue=1.0),
               booking("B2", updated_at="2025-05-01", batch_date="2025-09-02", revenue=2.0))
    pd.testing.assert_frame_equal(business(apply(b1, b2)), business(apply(b2, b1)))
    final = business(apply(b1, b2)).set_index("booking_id")["revenue"]
    assert final.to_dict() == {"B1": 1.0, "B2": 500.0}   # B2 in b2 was stale


def test_intra_batch_duplicates_collapse_to_latest():
    batch = frame(booking("B1", updated_at="2025-06-01", source_row=1, num_guests=2),
                  booking("B1", updated_at="2025-06-09", source_row=2, num_guests=4),
                  booking("B1", updated_at="2025-06-03", source_row=3, num_guests=3))
    result = merge_batch(empty_state(), batch)
    assert len(result.state) == 1
    assert result.state.iloc[0]["num_guests"] == 4
    assert result.counts["intra_batch_superseded"] == 2


def test_equal_updated_at_conflict_is_resolved_deterministically_and_flagged():
    a = booking("B1", source_row=20, revenue=32800.0)
    b = booking("B1", source_row=256, revenue=37800.0)
    r1 = merge_batch(empty_state(), frame(a, b))
    r2 = merge_batch(empty_state(), frame(b, a))       # input order must not matter
    assert r1.state.iloc[0]["revenue"] == r2.state.iloc[0]["revenue"] == 37800.0
    assert r1.counts["same_updated_at_conflicts"] == 1


def test_one_row_per_booking_id_after_many_batches():
    batches = [frame(*[booking(f"B{i}", updated_at=f"2025-06-{d:02d}",
                               batch_date=f"2025-09-{d:02d}") for i in range(5)])
               for d in (1, 3, 2)]
    state = apply(*batches)
    assert state["booking_id"].is_unique and len(state) == 5
    assert (state["updated_at"] == pd.Timestamp("2025-06-03")).all()


@pytest.mark.parametrize("field,value", [("revenue", 1.0), ("booking_status", "pending")])
def test_newer_version_with_changed_field_counts_as_update(field, value):
    state = apply(frame(booking("B1")))
    result = merge_batch(state, frame(booking("B1", updated_at="2025-06-02", **{field: value})))
    assert result.counts["updated"] == 1 and result.state.iloc[0][field] == value
