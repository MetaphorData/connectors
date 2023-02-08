from metaphor.common.tag_matcher import TagMatcher, match_tags, tag_datasets
from metaphor.models.metadata_change_event import (
    DataPlatform,
    Dataset,
    DatasetLogicalID,
)


def test_match_tags():

    matchers = [
        TagMatcher(pattern="*", tags=["tag1"]),
        TagMatcher(pattern="A.*", tags=["tag1", "tag2"]),
        TagMatcher(pattern="a.b.*", tags=["tag3"]),
        TagMatcher(
            pattern="x.*",
            tags=["tag4"],
        ),
    ]

    assert match_tags("a.b.c", matchers) == ["tag1", "tag2", "tag3"]


def test_tag_datasets():

    matchers = [
        TagMatcher(pattern="a.*", tags=["tag1", "tag2"]),
    ]

    d1 = Dataset(
        logical_id=DatasetLogicalID(
            platform=DataPlatform.BIGQUERY,
            name="a.b.c",
        )
    )

    d2 = Dataset(
        logical_id=DatasetLogicalID(
            platform=DataPlatform.BIGQUERY,
            name="x.y.z",
        )
    )

    tag_datasets([d1, d2], matchers)

    assert d1.tag_assignment.tag_names == ["tag1", "tag2"]
    assert d2.tag_assignment is None
