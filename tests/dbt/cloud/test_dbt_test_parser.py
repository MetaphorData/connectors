from datetime import datetime, timezone
from typing import Dict

from metaphor.dbt.cloud.discovery_api.generated.get_job_run_models import (
    GetJobRunModelsJobModels as Model,
)
from metaphor.dbt.cloud.discovery_api.generated.get_job_run_models import (
    GetJobRunModelsJobModelsColumns,
)
from metaphor.dbt.cloud.discovery_api.generated.get_job_run_models import (
    GetJobRunModelsJobModelsRunResults as RunResult,
)
from metaphor.dbt.cloud.discovery_api.generated.get_job_run_tests import (
    GetJobRunTestsJobTests as Test,
)
from metaphor.dbt.cloud.parser.dbt_test_parser import TestParser
from metaphor.models.metadata_change_event import (
    DataPlatform,
    Dataset,
    DbtModel,
    VirtualView,
)


def test_dbt_test_parser():
    virtual_views: Dict[str, VirtualView] = {}
    datasets: Dict[str, Dataset] = {}
    test_parser = TestParser(
        platform=DataPlatform.SNOWFLAKE,
        account="john.doe@metaphor.io",
        virtual_views=virtual_views,
        datasets=datasets,
    )
    models = {}
    test = Test(
        dependsOn=[],
        name="test",
        uniqueId="test.unique.id",
        compiledCode="compiledCode",
        compiledSql="compiledSql",
        columnName="column",
        status="warn",
        executeCompletedAt=datetime(2000, 1, 2, tzinfo=timezone.utc),
    )

    # No depends_on - nothing
    test_parser.parse(test, models)
    assert not virtual_views

    # No model in depends_on - nothing
    test.depends_on = ["foo"]
    test_parser.parse(test, models)
    assert not virtual_views

    # depends_on model not in virtual_views - nothing
    test.depends_on = ["model.foo"]
    test_parser.parse(test, models)
    assert not virtual_views

    # depends_on model not in models - nothing
    virtual_views["model.foo"] = VirtualView(dbt_model=DbtModel())
    test_parser.parse(test, models)
    dbt_model = virtual_views["model.foo"].dbt_model
    assert dbt_model and not dbt_model.tests

    # model is not qualified - no data quality monitor
    model = Model(
        runResults=[
            RunResult(
                status="pass",
                executeCompletedAt=datetime.now(),
            ),
        ],
        alias=None,
        columns=[
            GetJobRunModelsJobModelsColumns(
                comment=None,
                description=None,
                meta=None,
                name="col",
                tags=[],
                type="TEXT",
            ),
        ],
        compileCompletedAt=datetime.fromisoformat("2024-01-01T00:00:00"),
        compiledCode="compiledCode",
        compiledSql="compiledSql",
        database=None,
        dependsOn=[],
        description="description",
        environmentId=1234,
        materializedType="MATERIALIZED_VIEW",
        meta=None,
        name="foo",
        packageName="package",
        rawCode="rawCode",
        rawSql="rawSql",
        schema=None,
        tags=None,
        uniqueId="model.foo",
    )
    models["model.foo"] = model
    virtual_views["model.foo"] = VirtualView(dbt_model=DbtModel())
    test_parser.parse(test, models)
    assert not datasets

    # Test does not have name - no data quality monitor
    test.name = None
    virtual_views["model.foo"] = VirtualView(dbt_model=DbtModel())
    test_parser.parse(test, models)
    assert not datasets
