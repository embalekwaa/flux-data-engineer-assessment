"""Step 1: read every booking batch file that hasn't been processed yet."""
from flux_pipeline.config import load_config
from flux_pipeline.pipeline import load_new_batches

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data(*args, **kwargs):
    # Set the pipeline variable `force` to true to reprocess batches already loaded.
    force = str(kwargs.get('force', False)).lower() == 'true'
    return load_new_batches(load_config(), force=force)


@test
def test_output(output, *args) -> None:
    assert output is not None, 'Loader returned nothing'
