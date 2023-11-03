from datetime import datetime, timezone

from metaphor.power_bi.power_bi_utils import find_refresh_time_from_transaction
from tests.power_bi.test_extractor import fake_get_dataflow_transactions


def test_find_refresh_time_from_transaction():
    assert find_refresh_time_from_transaction([]) is None
    assert find_refresh_time_from_transaction(
        fake_get_dataflow_transactions()
    ) == datetime(2023, 10, 19, 1, 6, 10, 290000, tzinfo=timezone.utc)
