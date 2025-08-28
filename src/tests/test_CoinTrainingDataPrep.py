from lib.Utils import Utils
from services.logger.Logger import _log
from services.CoinTrainingDataPrep import CoinTrainingDataPrep

def test_list_available_raw_training_metadata():
    ct_data = CoinTrainingDataPrep()
    metadata = ct_data.list_available_raw_training_metadata()
    assert isinstance(metadata, list)
    for item in metadata:
        assert isinstance(item, dict)
        assert "symbol" in item
        assert "modified_timestamp" in item