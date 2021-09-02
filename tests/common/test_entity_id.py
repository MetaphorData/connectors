from metaphor.models.metadata_change_event import (
    DataPlatform,
    DatasetLogicalID,
    EntityType,
)

from metaphor.common.entity_id import EntityId


def test_to_str():
    id = EntityId(
        EntityType.DATASET,
        DatasetLogicalID(name="name", platform=DataPlatform.SNOWFLAKE),
    )

    # md5({"name":"name","platform":"SNOWFLAKE"}) == B1B4CE1961D6D6C4427DC8F1D9F4EF34
    assert str(id) == "DATASET~B1B4CE1961D6D6C4427DC8F1D9F4EF34"

    id = EntityId(
        EntityType.DATASET,
        DatasetLogicalID(
            name="name", account="account", platform=DataPlatform.SNOWFLAKE
        ),
    )

    # md5({"account":"account","name":"name","platform":"SNOWFLAKE"}) == 5AC8814ADBAFDA3D2B6D1AC58782C5D4
    assert str(id) == "DATASET~5AC8814ADBAFDA3D2B6D1AC58782C5D4"
