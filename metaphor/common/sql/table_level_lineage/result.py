from typing import List

from pydantic.dataclasses import dataclass

from metaphor.models.metadata_change_event import QueriedDataset


@dataclass
class Result:
    targets: List[QueriedDataset] = []
    sources: List[QueriedDataset] = []
