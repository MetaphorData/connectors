from metaphor.common.sql.utils import is_valid_queried_datasets
from metaphor.models.metadata_change_event import QueriedDataset


def test_is_valid_queried_datasets():
    assert is_valid_queried_datasets([]) is True
    assert (
        is_valid_queried_datasets(
            [QueriedDataset(database="db", schema="schema", table="table")]
        )
        is True
    )
    assert (
        is_valid_queried_datasets([QueriedDataset(schema="schema", table="table")])
        is False
    )
    assert (
        is_valid_queried_datasets(
            [QueriedDataset(schema="schema", table="table")], ignore_database=True
        )
        is True
    )
