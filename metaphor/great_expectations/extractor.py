from typing import Collection, Dict, List, Optional

import great_expectations as gx
from great_expectations.core.batch import LegacyBatchDefinition
from great_expectations.core.batch_spec import BatchSpec
from great_expectations.core.expectation_validation_result import (
    ExpectationSuiteValidationResult,
)
from great_expectations.datasource.fluent import DataAsset
from great_expectations.execution_engine import SqlAlchemyExecutionEngine
from sqlalchemy import URL

from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.entity_id import (
    dataset_normalized_name,
    parts_to_dataset_entity_id,
    to_dataset_entity_id_from_logical_id,
)
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.logger import get_logger
from metaphor.great_expectations.config import GreatExpectationConfig
from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import (
    DataMonitor,
    DataMonitorStatus,
    DataMonitorTarget,
    DataPlatform,
    DataQualityProvider,
    Dataset,
    DatasetDataQuality,
    DatasetLogicalID,
)

logger = get_logger()


class GreatExpectationsExtractor(BaseExtractor):
    """
    Great Expectations metadata extractor. The extractor runs by
    parsing existing Great Expectations data context, so make sure the
    execution context has been persisted. In other words, it will not
    work if you get Great Expectations context like this in your script:
    ```python
    ctx = gx.get_context() # This creates a context in memory, and nothing is persisted
    ```
    """

    _description = "Great Expectations metadata crawler"
    _platform = Platform.GREAT_EXPECTATIONS

    @staticmethod
    def from_config_file(config_file: str) -> "GreatExpectationsExtractor":
        return GreatExpectationsExtractor(
            GreatExpectationConfig.from_yaml_file(config_file)
        )

    def __init__(self, config: GreatExpectationConfig) -> None:
        super().__init__(config)
        self._config = config
        self._datasets: Dict[str, Dataset] = {}

    async def extract(self) -> Collection[ENTITY_TYPES]:
        self.context = gx.get_context(
            project_root_dir=self._config.project_root_dir, mode="file"
        )
        for validation_result in self.context.validation_results_store.get_all():
            if isinstance(validation_result, ExpectationSuiteValidationResult):
                self._parse_suite_result(validation_result)
        return self._datasets.values()

    @staticmethod
    def _get_dataset_key(
        platform: DataPlatform,
        account: Optional[str],
        database: Optional[str],
        schema: Optional[str],
        table: str,
    ):
        return str(
            parts_to_dataset_entity_id(platform, account, database, schema, table)
        )

    def _init_dataset(
        self,
        platform: DataPlatform,
        account: Optional[str],
        database: Optional[str],
        schema: Optional[str],
        table: str,
    ) -> Dataset:
        key = self._get_dataset_key(platform, account, database, schema, table)
        dataset_name = dataset_normalized_name(database, schema, table)
        if key not in self._datasets:
            self._datasets[key] = Dataset(
                logical_id=DatasetLogicalID(
                    account=account,
                    name=dataset_name,
                    platform=platform,
                ),
            )

        dataset = self._datasets[key]
        if dataset.data_quality is None:
            dataset.data_quality = DatasetDataQuality(
                provider=DataQualityProvider.GREAT_EXPECTATIONS,
                monitors=[],
            )
        return dataset

    def _parse_suite_result(
        self, validation_result: ExpectationSuiteValidationResult
    ) -> None:
        logger.info(f"Parsing validation result: {validation_result.id}")
        active_batch_definition: LegacyBatchDefinition = validation_result.meta[
            "active_batch_definition"
        ]

        datasource = self.context.data_sources.get(
            active_batch_definition["datasource_name"]
        )
        execution_engine = datasource.get_execution_engine()

        # TODO: support PandasExecutionEngine
        # TODO: support SparkDFExecutionEngine
        if not isinstance(execution_engine, SqlAlchemyExecutionEngine):
            logger.warning(
                f"Cannot process execution engine: {execution_engine}, not parsing this validation result"
            )
            return

        self._parse_sql_execution_engine_result(
            validation_result,
            execution_engine,
            datasource.get_asset(active_batch_definition["data_asset_name"]),
        )

    def _parse_sql_execution_engine_result(
        self,
        validation_result: ExpectationSuiteValidationResult,
        execution_engine: SqlAlchemyExecutionEngine,
        data_asset: DataAsset,
    ) -> None:
        # batch_spec is always just a dict, using isinstance to get its type will not work
        batch_spec: BatchSpec = validation_result.meta["batch_spec"]
        logger.info(f"batch spec: {batch_spec}")

        if (
            "query" in batch_spec
            or "batch_data" in batch_spec
            or "schema_name" not in batch_spec
            and "table_name" not in batch_spec
        ):
            # If "query" is in batch spec, then it is a RuntimeQueryBatchSpec, we should parse
            # the query and see what datasets are referenced in it.
            #
            # If "batch_data" is in batch spec, then it is a RuntimeDataBatchSpec and it's just
            # a file in the filesystem.
            #
            # If batch spec does not have "schema_name" nor "table_name", there's no way for us
            # to get a fully qualified name for the dataset, and we're just gonna ignore it.
            logger.warning("Unsupported batch spec, not parsing this validation result")
            return

        url = execution_engine.engine.url
        backend = url.get_backend_name().upper()
        platform = next((x for x in DataPlatform if x.value == backend), None)
        if not platform:
            logger.warning(
                f"Unknown SqlAlchemy backend: {backend}, not parsing this validation result"
            )
            return

        account = (
            self._config.snowflake_account
            if platform is DataPlatform.SNOWFLAKE
            else None
        )
        database = self._extract_database_from_sqlalchemy_url(url, platform)
        schema = batch_spec.get("schema_name")
        table = batch_spec.get("table_name")

        dataset = self._init_dataset(
            platform,
            account,
            database,
            schema,
            table or data_asset.name,
        )

        assert dataset.data_quality and dataset.data_quality.monitors is not None

        # Right now the whole suite is a single DataMonitor, so if one expectation fails
        # the whole monitor fails.
        # TODO: decide if we want to make a DataMonitor for each `validation_result.result`.
        dataset.data_quality.monitors.append(
            DataMonitor(
                title=validation_result.suite_name,
                status=(
                    DataMonitorStatus.PASSED
                    if validation_result.success
                    else DataMonitorStatus.ERROR
                ),
                targets=self._parse_result_targets(validation_result, dataset),
                url=validation_result.result_url,
                exceptions=self._parse_result_exceptions(validation_result),
            )
        )

    @staticmethod
    def _parse_result_targets(
        validation_result: ExpectationSuiteValidationResult, dataset: Dataset
    ) -> Optional[List[DataMonitorTarget]]:
        assert dataset.logical_id and dataset.logical_id.name
        targets = [
            DataMonitorTarget(
                dataset=str(to_dataset_entity_id_from_logical_id(dataset.logical_id)),
                column=result.expectation_config.kwargs["column"],
            )
            for result in validation_result.results
            if result.expectation_config
            and result.expectation_config.kwargs.get("column")
        ]
        return targets or None

    @staticmethod
    def _parse_result_exceptions(
        validation_result: ExpectationSuiteValidationResult,
    ) -> Optional[List[str]]:
        exceptions = [
            result.exception_info["exception_message"]
            for result in validation_result.results
            if result.exception_info
            and result.exception_info.get("raised_exception", False)
        ]
        return exceptions or None

    @staticmethod
    def _extract_database_from_sqlalchemy_url(url: URL, platform: DataPlatform) -> str:
        """
        Reference:
        https://docs.greatexpectations.io/docs/core/connect_to_data/sql_data/#procedure
        """

        if platform is DataPlatform.SNOWFLAKE:
            # GX connect string for Snowflake looks like
            # snowflake://<USER_NAME>:<PASSWORD>@<ACCOUNT_NAME>/<DATABASE_NAME>/<SCHEMA_NAME>?warehouse=<WAREHOUSE_NAME>&role=<ROLE_NAME>&application=great_expectations_oss
            # And SQLAlchemy URL considers whatever is behind `ACCOUNT_NAME` the database of the url.
            #
            # We want to extract `SCHEMA_NAME` from `DATABASE_NAME/SCHEMA_NAME`.
            return (url.database or "").rsplit("/", maxsplit=1)[0]

        if platform is DataPlatform.POSTGRESQL:
            # PostgreSQL connect string:
            # postgresql+psycopg2://<USERNAME>:<PASSWORD>@<HOST>:<PORT>/<DATABASE>
            return url.database or ""

        database = url.database or ""
        logger.info(f"Using {database} for platform = {platform.value}")
        return database
