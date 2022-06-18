from datetime import datetime, timezone
from pathlib import Path
from typing import Dict

from metaphor.models.metadata_change_event import (
    DataPlatform,
    Dataset,
    DatasetStatistics,
    VirtualView,
)

from metaphor.common.logger import get_logger
from metaphor.dbt.config import DbtRunConfig
from metaphor.dbt.generated.dbt_catalog_v1 import CatalogTable, DbtCatalog
from metaphor.dbt.util import (
    build_docs_url,
    init_dataset,
    init_documentation,
    init_field,
    init_field_doc,
    init_virtual_view,
)

logger = get_logger(__name__)


class CatalogParserV1:
    """
    dbt catalog parser, using v1 schema https://schemas.getdbt.com/dbt/catalog/v1.json
    """

    def __init__(
        self,
        config: DbtRunConfig,
        platform: DataPlatform,
        datasets: Dict[str, Dataset],
        virtual_views: Dict[str, VirtualView],
    ):
        self._platform = platform
        self._account = config.account
        self._docs_base_url = config.docs_base_url
        self._datasets = datasets
        self._virtual_views = virtual_views

    def parse(self, catalog_file: str) -> None:
        try:
            catalog = DbtCatalog.parse_file(Path(catalog_file))
        except Exception as e:
            logger.error(f"Parse catalog json error: {e}")
            return

        for node in catalog.nodes.values():
            self._parse_catalog_model(node)

        for source in catalog.sources.values():
            self._parse_catalog_source(source)

    def _parse_catalog_model(self, model: CatalogTable) -> None:
        assert model.unique_id is not None

        # Catalog nodes can be either models or seeds.
        # The only way to distinguish them is by their ID prefix
        if not model.unique_id.startswith("model."):
            return

        virtual_view = init_virtual_view(self._virtual_views, model.unique_id)
        dbt_model = virtual_view.dbt_model

        dbt_model.description = dbt_model.description or model.metadata.comment
        dbt_model.docs_url = dbt_model.docs_url or build_docs_url(
            self._docs_base_url, model.unique_id
        )

        for col in model.columns.values():
            column_name = col.name.lower()
            field = init_field(dbt_model.fields, column_name)
            field.description = field.description or col.comment
            field.native_type = field.native_type or col.type or "Not Set"

    def _parse_catalog_source(self, model: CatalogTable) -> None:
        meta = model.metadata
        if not meta.database or not model.unique_id:
            return

        dataset = init_dataset(
            self._datasets,
            meta.database,
            meta.schema_,
            meta.name,
            self._platform,
            self._account,
            model.unique_id,
        )

        # TODO (ch1236): Re-enable once we figure the source & expected format
        # self._init_ownership(dataset)
        # assert dataset.ownership is not None and dataset.ownership.people is not None
        # dataset.ownership.people.append(self._build_owner(meta["owner"]))

        init_documentation(dataset)
        if meta.comment:
            dataset.documentation.dataset_documentations = (
                dataset.documentation.dataset_documentations or [meta.comment]
            )

        for col in model.columns.values():
            if col.comment:
                column_name = col.name.lower()
                field_doc = init_field_doc(dataset, column_name)
                field_doc.documentation = field_doc.documentation or col.comment

    @staticmethod
    def _parse_catalog_statistics(dataset: Dataset, model: CatalogTable) -> None:
        stats = model.stats

        has_stats = stats.get("has_stats")
        if has_stats is not None and has_stats.value is not None:
            statistics = DatasetStatistics()
            found_statistics = False

            row_count = stats.get("row_count")
            if row_count is not None and row_count.value is not None:
                found_statistics = True
                statistics.record_count = float(row_count.value)

            bytes = stats.get("bytes")
            if bytes is not None and bytes.value is not None:
                found_statistics = True
                statistics.data_size = (
                    float(bytes.value) / 1048576  # convert bytes to MB
                )

            last_modified = stats.get("last_modified")
            if last_modified is not None and last_modified.value is not None:
                found_statistics = True
                if isinstance(last_modified.value, str):
                    # Must set tzinfo explicitly due to https://bugs.python.org/issue22377
                    statistics.last_updated = datetime.strptime(
                        last_modified.value, "%Y-%m-%d %H:%M%Z"
                    ).replace(tzinfo=timezone.utc)
                else:
                    statistics.last_updated = datetime.fromtimestamp(
                        last_modified.value
                    ).replace(tzinfo=timezone.utc)

            if found_statistics:
                dataset.statistics = statistics
