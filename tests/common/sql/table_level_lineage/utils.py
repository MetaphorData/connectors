from typing import Optional, Set

from metaphor.common.sql.table_level_lineage.table_level_lineage import (
    extract_table_level_lineage,
)
from metaphor.models.metadata_change_event import DataPlatform, QueriedDataset


def _get_dataset_name(dataset: QueriedDataset):
    return ".".join(x for x in [dataset.database, dataset.schema, dataset.table] if x)


def assert_table_lineage_equal(
    sql: str,
    source_tables: Optional[Set[str]],
    target_tables: Optional[Set[str]],
    platform: DataPlatform = DataPlatform.UNKNOWN,
    account: Optional[str] = None,
    statement_type: Optional[str] = None,
):
    res = extract_table_level_lineage(sql, platform, account, statement_type, None)
    expected_sources = sorted(source_tables) if source_tables else []
    actual_sources = sorted(_get_dataset_name(source) for source in res.sources)
    assert (
        actual_sources == expected_sources
    ), f"Expected sources: {expected_sources}, actual sources: {actual_sources}"
    expected_targets = sorted(target_tables) if target_tables else []
    actual_targets = sorted(_get_dataset_name(target) for target in res.targets)
    assert (
        actual_targets == expected_targets
    ), f"Expected targets: {expected_targets}, actual targets: {actual_targets}"
