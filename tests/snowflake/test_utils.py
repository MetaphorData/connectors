from metaphor.models.metadata_change_event import (
    Dataset,
    SystemTag,
    SystemTags,
    SystemTagSource,
)
from metaphor.snowflake.utils import dedup_dataset_system_tags, to_quoted_identifier


def test_to_quoted_identifier():
    assert to_quoted_identifier([None, "", "a", "b", "c"]) == '"a"."b"."c"'

    assert to_quoted_identifier(["db", "sc", 'ta"@BLE']) == '"db"."sc"."ta""@BLE"'


def test_dedup_dataset_system_tags():
    dataset = Dataset(
        system_tags=SystemTags(
            tags=[
                SystemTag(
                    key="a",
                    system_tag_source=SystemTagSource.SNOWFLAKE,
                    value="b",
                ),
                SystemTag(
                    key="a",
                    system_tag_source=SystemTagSource.SNOWFLAKE,
                    value="b",
                ),
                SystemTag(
                    key="aa",
                    system_tag_source=SystemTagSource.SNOWFLAKE,
                    value="bc",
                ),
                SystemTag(
                    key="a",
                    system_tag_source=SystemTagSource.SNOWFLAKE,
                    value="b",
                ),
            ]
        )
    )

    dedup_dataset_system_tags(dataset)
    assert dataset.system_tags and dataset.system_tags.tags
    assert len(dataset.system_tags.tags) == 2
    assert (
        SystemTag(key="a", system_tag_source=SystemTagSource.SNOWFLAKE, value="b")
        in dataset.system_tags.tags
    )
    assert (
        SystemTag(key="aa", system_tag_source=SystemTagSource.SNOWFLAKE, value="bc")
        in dataset.system_tags.tags
    )
