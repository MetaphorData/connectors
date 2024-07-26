from datetime import datetime
from typing import Dict, List, Optional

from metaphor.common.logger import get_logger
from metaphor.dbt.cloud.discovery_api.generated.get_job_run_models import (
    GetJobRunModelsJobModels as Model,
)
from metaphor.dbt.cloud.discovery_api.generated.get_job_run_models import (
    GetJobRunModelsJobModelsRunResults as RunResult,
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
            logger.warn(
                f"Test {test.unique_id} references non-active model {model_unique_id}"
            )
            return

        if model_unique_id not in models:
            return

        model = models[model_unique_id]

        dbt_test = DbtTest(
            name=test.name,
            unique_id=test.unique_id,
            columns=[test.column_name] if test.column_name else [],
            depends_on_macros=[n for n in test.depends_on if n.startswith("macro.")],
        )

        # V7 renamed "compiled_sql" to "compiled_code"
        dbt_test.sql = test.compiled_code or test.compiled_sql

        init_dbt_tests(self._virtual_views, model_unique_id).append(dbt_test)

        if model.run_results:
            self._parse_test_run_result(
                test, models[model_unique_id], model.run_results
            )

    @staticmethod
    def _get_run_result_executed_completed_at(
        run_result: RunResult,
    ) -> Optional[datetime]:
        if isinstance(run_result.execute_completed_at, datetime):
            return run_result.execute_completed_at
        if isinstance(run_result.execute_completed_at, str):
            completed_at = run_result.execute_completed_at
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
        run_results: List[RunResult],
    ) -> None:
        model_name = model.alias or model.name
        if model.database is None or model.schema_ is None or model_name is None:
            logger.warning(f"Skipping model without name, {model.unique_id}")
            return

        if not test.name:
            return

        if not run_results:
            logger.warning(f"Skipping test without run_results, {model.unique_id}")
            return

        def run_result_key(run_result: RunResult):
            completed_at = self._get_run_result_executed_completed_at(run_result)
            if not completed_at:
                return 0
            return completed_at.timestamp()

        run_result = next(
            (
                n
                for n in sorted(run_results, key=run_result_key, reverse=True)
                if n.status
            ),
            None,
        )
        if run_result is None or run_result.status is None:
            logger.warning(f"No valid run_result found: {run_results}")
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

        status = dbt_run_result_output_data_monitor_status_map[run_result.status]
        last_run = self._get_run_result_executed_completed_at(run_result)
        add_data_quality_monitor(dataset, test.name, test.column_name, status, last_run)
