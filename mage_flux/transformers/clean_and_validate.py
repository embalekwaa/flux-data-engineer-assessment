"""Step 2: clean the rows, add FX + property details, and flag bad rows.

Bad rows aren't dropped here; each row gets a `_reject_reasons` value
('' means the row is fine).

Quality gate: if a batch has too many bad rows, this block raises an error,
which stops the pipeline *before* the merge. (It raises instead of using a
@test because a block error always blocks downstream blocks, in every way
Mage can run a pipeline.)
"""
from flux_pipeline import io
from flux_pipeline.config import load_config
from flux_pipeline.pipeline import clean_and_validate, reject_rate

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    cfg = load_config()
    checked = clean_and_validate(data, cfg, io.read_fx_rates(cfg), io.read_properties(cfg))

    rates = reject_rate(checked)
    too_high = rates[rates > cfg.max_reject_rate_fail]
    if not too_high.empty:
        raise ValueError(f'Too many bad rows (limit {cfg.max_reject_rate_fail:.0%}), '
                         f'stopping before merge: {too_high.round(3).to_dict()}')
    return checked


@test
def test_output(output, *args) -> None:
    assert '_reject_reasons' in output.columns or output.empty, 'Validation did not run'
