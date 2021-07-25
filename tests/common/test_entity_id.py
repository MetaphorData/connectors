from metaphor.models.metadata_change_event import (
    DataPlatform,
    DatasetLogicalID,
    EntityType,
)

from metaphor.common.entity_id import EntityId


def test_to_str():
    id = EntityId(EntityType.DATASET, DatasetLogicalID("name", DataPlatform.SNOWFLAKE))

    # md5({"name":"name","platform":"SNOWFLAKE"}) == B1B4CE1961D6D6C4427DC8F1D9F4EF34
    assert str(id) == "DATASET~B1B4CE1961D6D6C4427DC8F1D9F4EF34"
