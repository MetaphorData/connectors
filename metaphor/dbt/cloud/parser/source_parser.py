from typing import Dict, List, Optional

from metaphor.dbt.cloud.discovery_api import DiscoveryAPIClient
from metaphor.dbt.cloud.discovery_api.generated.get_sources import (
    GetSourcesEnvironmentAppliedSourcesEdgesNode as Node,
)
from metaphor.dbt.cloud.parser.common import DISCOVERY_API_PAGE_LIMIT
from metaphor.dbt.util import init_dataset, init_documentation
from metaphor.models.metadata_change_event import (
    DataPlatform,
    Dataset,
    FieldDocumentation,
)


class SourceParser:
    def __init__(
        self,
        discovery_api: DiscoveryAPIClient,
        datasets: Dict[str, Dataset],
        platform: DataPlatform,
        account: Optional[str],
    ):
        self._discovery_api = discovery_api
        self._datasets = datasets
        self._platform = platform
        self._account = account

    def _parse_source(self, source: Node) -> None:
        if not source.database or not source.schema_ or not source.identifier:
            return None

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
        documentation = dataset.documentation
        assert (
            documentation is not None
            and documentation.dataset_documentations is not None
            and documentation.field_documentations is not None
        )

        if source.description:
            documentation.dataset_documentations.append(source.description)

        field_documentations = documentation.field_documentations

        if source.catalog and source.catalog.columns:
            for col in source.catalog.columns:
                if not col.name or not col.description:
                    continue
                column_name = col.name.lower()

                field_documentation = next(
                    (
                        field_documentation
                        for field_documentation in field_documentations
                        if field_documentation.field_path == column_name
                    ),
                    None,
                )

                if field_documentation is None:
                    field_documentations.append(
                        FieldDocumentation(
                            documentation=col.description, field_path=column_name
                        )
                    )
                else:
                    field_documentation.documentation = col.description

        if (
            not documentation.dataset_documentations
            and not documentation.field_documentations
        ):
            # remove documentation if it's empty
            dataset.documentation = None

    def _get_sources_in_environment(self, environment_id: int) -> List[Node]:
        """
        return a list of source nodes
        """
        source_nodes: List[Node] = []
        after = None

        while True:
            result = self._discovery_api.get_sources(
                environment_id, first=DISCOVERY_API_PAGE_LIMIT, after=after
            )
            applied = result.environment.applied
            if not applied or not applied.sources:
                break
            source_nodes += [e.node for e in applied.sources.edges]
            after = applied.sources.page_info.end_cursor
            if not applied.sources.page_info.has_next_page:
                break

        return source_nodes

    def parse(self, environment_id: int) -> None:
        """
        Fetch and parse sources in a given environment
        parsed datasets are stored in datasets dict, also return the data platform
        """
        sources = self._get_sources_in_environment(environment_id)

        for source in sources:
            self._parse_source(source)
