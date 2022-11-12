from metaphor.common.entity_id import EntityId, dataset_normalized_name
from metaphor.models.metadata_change_event import (
    DataPlatform,
    DatasetLogicalID,
    EntityType,
)


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


def test_dataset_normalized_name():
    # should lower case
    assert "a.b.c" == dataset_normalized_name("A", "b", "C")

    # 2 segment format
    assert "b.c" == dataset_normalized_name("b", "C")

    # named argument
    assert "b.c" == dataset_normalized_name(schema="b", table="C")
    assert "a.c" == dataset_normalized_name(db="A", table="C")

    # should strip backquote
    assert "a.b.c" == dataset_normalized_name("`A`", "b", "C")

    # should not strip backquote
    assert "`a`.b.c" == dataset_normalized_name("`A`", "b", "C", strip_backquote=False)

    # should not lower case
    assert "A.b.C" == dataset_normalized_name("A", "b", "C", lower_case=False)
