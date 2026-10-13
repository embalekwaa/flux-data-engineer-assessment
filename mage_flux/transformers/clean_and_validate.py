"""Step 2: clean the rows, add FX + property details, and flag bad rows.

Bad rows aren't dropped here; each row gets a `_reject_reasons` value
('' means the row is fine).
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
    return clean_and_validate(data, cfg, io.read_fx_rates(cfg), io.read_properties(cfg))


@test
def test_reject_rate_below_limit(output, *args) -> None:
    """Quality gate: if a batch is mostly bad rows, stop here, before the merge."""
    limit = load_config().max_reject_rate_fail
    rates = reject_rate(output)
    too_high = rates[rates > limit]
    assert too_high.empty, f'Too many bad rows (limit {limit:.0%}): {too_high.round(3).to_dict()}'
