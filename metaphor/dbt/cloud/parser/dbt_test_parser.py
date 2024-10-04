from datetime import datetime
from typing import Any, Dict, Optional

from metaphor.common.logger import get_logger
from metaphor.dbt.cloud.discovery_api.generated.get_job_run_models import (
    GetJobRunModelsJobModels as Model,
)
from metaphor.dbt.cloud.discovery_api.generated.get_job_run_tests import (
    GetJobRunTestsJobTests as Test,
)
from metaphor.dbt.util import add_data_quality_monitor, init_dataset, init_dbt_tests
from metaphor.models.metadata_change_event import (
    DataMonitorStatus,
    DataPlatform,
    Dataset,
    DbtTest,
    VirtualView,
)

logger = get_logger()
dbt_run_result_output_data_monitor_status_map: Dict[str, DataMonitorStatus] = {
    "warn": DataMonitorStatus.WARNING,
    "skipped": DataMonitorStatus.UNKNOWN,
    "error": DataMonitorStatus.ERROR,
    "fail": DataMonitorStatus.ERROR,
    "runtime error": DataMonitorStatus.ERROR,
    "pass": DataMonitorStatus.PASSED,
    "success": DataMonitorStatus.PASSED,
}


class TestParser:
    def __init__(
        self,
        platform: DataPlatform,
        account: Optional[str],
        virtual_views: Dict[str, VirtualView],
        datasets: Dict[str, Dataset],
    ) -> None:
        self._platform = platform
        self._account = account
        self._virtual_views = virtual_views
        self._datasets = datasets

    def parse(
        self,
        test: Test,
        models: Dict[str, Model],
    ) -> None:
        # check test is referring a model
        if not test.depends_on:
            return

        model_unique_id = next(
            (n for n in test.depends_on if n.startswith("model.")), None
        )
        if not model_unique_id:
            return

        # Skip test if it references an non-existing (most likely disabled) model
        if model_unique_id not in self._virtual_views:
            logger.warning(
                f"Test {test.unique_id} references non-active model {model_unique_id}"
            )
            return

        if model_unique_id not in models:
            return

        dbt_test = DbtTest(
            name=test.name,
            unique_id=test.unique_id,
            columns=[test.column_name] if test.column_name else [],
            depends_on_macros=[n for n in test.depends_on if n.startswith("macro.")],
        )

        # V7 renamed "compiled_sql" to "compiled_code"
        dbt_test.sql = test.compiled_code or test.compiled_sql

        init_dbt_tests(self._virtual_views, model_unique_id).append(dbt_test)

        self._parse_test_run_result(test, models[model_unique_id])

    @staticmethod
    def _parse_date_time_from_result(
        field: Optional[Any],
    ) -> Optional[datetime]:
        if isinstance(field, datetime):
            return field
        if isinstance(field, str):
            completed_at = field
            if completed_at.endswith("Z"):
                # Convert Zulu to +00:00
                completed_at = f"{completed_at[:-1]}+00:00"
            try:
                return datetime.fromisoformat(completed_at)
            except Exception:
                return None
        return None

    def _parse_test_run_result(
        self,
        test: Test,
        model: Model,
    ) -> None:
        model_name = model.alias or model.name
        if model.database is None or model.schema_ is None or model_name is None:
            logger.warning(f"Skipping model without name, {model.unique_id}")
            return

        if not test.name or test.status is None or test.execute_completed_at is None:
            return

        dataset = init_dataset(
            self._datasets,
            model.database,
            model.schema_,
            model_name,
            self._platform,
            self._account,
            model.unique_id,
        )

        status = dbt_run_result_output_data_monitor_status_map[test.status]
        last_run = self._parse_date_time_from_result(test.execute_completed_at)
        add_data_quality_monitor(dataset, test.name, test.column_name, status, last_run)
