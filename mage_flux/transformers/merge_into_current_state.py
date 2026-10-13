"""Step 3: merge each batch into the current-state table (one row per booking).

The newest version of a booking wins (by updated_at), so re-running is safe:
re-sent rows change nothing and older updates never overwrite newer ones.
Returns the merge audit: one row per batch with inserted / updated / unchanged counts.
"""
import pandas as pd

from flux_pipeline.config import load_config
from flux_pipeline.pipeline import AUDIT_COLUMNS, merge_and_save, new_run_id

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    audit_rows = merge_and_save(data, load_config(), new_run_id())
    return pd.DataFrame(audit_rows, columns=AUDIT_COLUMNS)


@test
def test_every_row_accounted_for(output, *args) -> None:
    """Each valid row must be exactly one of: superseded, inserted, updated, unchanged, stale."""
    parts = ['intra_batch_superseded', 'inserted', 'updated', 'unchanged', 'stale_ignored']
    assert (output[parts].sum(axis=1) == output['rows_valid']).all(), 'Merge counts do not add up'
