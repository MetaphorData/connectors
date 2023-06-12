from fnmatch import fnmatch
from typing import List

from pydantic.dataclasses import dataclass

from metaphor.common.dataclass import ConnectorConfig
from metaphor.common.utils import unique_list
from metaphor.models.metadata_change_event import Dataset, TagAssignment


@dataclass(config=ConnectorConfig)
class TagMatcher:
    # The glob pattern to use when matching against the asset name
    pattern: str

    # Tags to assign when matched
    tags: List[str]


def match_tags(name: str, matchers: List[TagMatcher]) -> List[str]:
    """Returns an unique list of matched tags based on the name"""

    tags = []
    for m in matchers:
        if fnmatch(name, m.pattern.lower()):
            tags.extend(m.tags)

    return unique_list(tags)


def tag_datasets(datasets: List[Dataset], matchers: List[TagMatcher]):
    """Assign matched tags to datasets"""

    if len(matchers) == 0:
        return

    for dataset in datasets:
        matched_tags = match_tags(dataset.logical_id.name, matchers)
        if len(matched_tags) > 0:
            dataset.tag_assignment = TagAssignment(tag_names=matched_tags)
