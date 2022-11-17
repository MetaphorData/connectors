from metaphor.common.models import DeserializableDatasetLogicalID
from metaphor.models.metadata_change_event import DataPlatform, DatasetLogicalID


def test_to_logical_id():
    id = DeserializableDatasetLogicalID(
        name="A.b.C",
        platform="SNOWFLAKE",
        account="account",
    )
    assert id.to_logical_id() == DatasetLogicalID(
        name="a.b.c", platform=DataPlatform.SNOWFLAKE, account="account"
    )


def test_to_entity_id():
    id = DeserializableDatasetLogicalID(
        name="A.b.C",
        platform="SNOWFLAKE",
        account="account",
    )
    assert str(id.to_entity_id()) == "DATASET~F6E5D45A920F34DC65C17EF4FE00FF41"
