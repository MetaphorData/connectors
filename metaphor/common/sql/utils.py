from typing import List

from metaphor.models.metadata_change_event import QueriedDataset


def is_valid_queried_datasets(
    datasets: List[QueriedDataset], ignore_database=False
) -> bool:
    """
    database and schema must exist and not an empty string
    """
    for dataset in datasets:
        if not ignore_database and not dataset.database:
            return False
        if not dataset.schema or not dataset.table:
            return False
    return True
