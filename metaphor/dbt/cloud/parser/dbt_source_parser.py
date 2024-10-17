from typing import Dict, List, Optional

from metaphor.common.entity_id import EntityId, parts_to_dataset_entity_id
from metaphor.dbt.cloud.discovery_api.generated.get_job_run_sources import (
    GetJobRunSourcesJobSources,
)
from metaphor.dbt.util import init_dataset, init_documentation
from metaphor.models.metadata_change_event import (
    DataPlatform,
    Dataset,
    FieldDocumentation,
)


class SourceParser:
    def __init__(
        self,
        datasets: Dict[str, Dataset],
        platform: DataPlatform,
        account: Optional[str],
    ):
        self._datasets = datasets
        self._platform = platform
        self._account = account

    def _parse_source(self, source: GetJobRunSourcesJobSources) -> None:
        if (
            not source.database
            or not source.columns
            or not source.schema_
            or not source.identifier
        ):
            return

        dataset = init_dataset(
            self._datasets,
            source.database,
            source.schema_,
            source.identifier,
            self._platform,
            self._account,
            source.unique_id,
        )

        init_documentation(dataset)
        assert (
            dataset.documentation is not None
            and dataset.documentation.dataset_documentations is not None
            and dataset.documentation.field_documentations is not None
        )
        if source.description:
            dataset.documentation.dataset_documentations.append(source.description)

        for col in source.columns:
            if col.description:
                if not col.name:
                    continue
                column_name = col.name.lower()
                if not any(
                    field_documentation.field_path == column_name
                    and field_documentation.documentation == col.description
                    for field_documentation in dataset.documentation.field_documentations
                    or []
                ):
                    dataset.documentation.field_documentations.append(
                        FieldDocumentation(
                            documentation=col.description, field_path=column_name
                        )
                    )

    def parse(self, sources: List[GetJobRunSourcesJobSources]) -> Dict[str, EntityId]:
        source_map: Dict[str, EntityId] = {}
        for source in sources:
            assert source.database is not None and source.schema_ and source.identifier
            source_map[source.unique_id] = parts_to_dataset_entity_id(
                self._platform,
                self._account,
                source.database,
                source.schema_,
                source.identifier,
            )
            self._parse_source(source)

        return source_map
