"""Step 4: write the output files (analysis-ready table, rejects, DQ summary, merge audit).

Fails the run if any data-quality check has status FAIL, so Mage marks the
pipeline as failed and can alert.
"""
from flux_pipeline import io
from flux_pipeline.config import load_config
from flux_pipeline.dq import has_failures
from flux_pipeline.pipeline import publish

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data(data, *args, **kwargs):
    cfg = load_config()
    dq = publish(cfg, data.to_dict('records'), io.read_fx_rates(cfg), io.read_properties(cfg))
    if has_failures(dq):
        failed = dq[dq['status'] == 'FAIL'][['check', 'scope', 'value']]
        raise ValueError(f'Data-quality checks failed:\n{failed.to_string(index=False)}')
