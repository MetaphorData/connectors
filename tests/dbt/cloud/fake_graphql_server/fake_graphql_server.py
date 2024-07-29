from tests.dbt.cloud.fake_graphql_server.get_environment_adapter_type.endpoint import (
    fake_GetEnvironmentAdapterType,
)
from tests.dbt.cloud.fake_graphql_server.get_job_run_macros.endpoint import (
    fake_GetJobRunMacros,
)
from tests.dbt.cloud.fake_graphql_server.get_job_run_metrics.endpoint import (
    fake_GetJobRunMetrics,
)
from tests.dbt.cloud.fake_graphql_server.get_job_run_models.endpoint import (
    fake_GetJobRunModels,
)
from tests.dbt.cloud.fake_graphql_server.get_job_run_snapshots.endpoint import (
    fake_GetJobRunSnapshots,
)
from tests.dbt.cloud.fake_graphql_server.get_job_run_sources.endpoint import (
    fake_GetJobRunSources,
)
from tests.dbt.cloud.fake_graphql_server.get_job_run_tests.endpoint import (
    fake_GetJobRunTests,
)
from tests.dbt.cloud.fake_graphql_server.get_macro_arguments.endpoint import (
    fake_GetMacroArguments,
)

endpoints = {
    "GetEnvironmentAdapterType": fake_GetEnvironmentAdapterType,
    "GetJobRunMacros": fake_GetJobRunMacros,
    "GetJobRunMetrics": fake_GetJobRunMetrics,
    "GetJobRunModels": fake_GetJobRunModels,
    "GetJobRunSnapshots": fake_GetJobRunSnapshots,
    "GetJobRunSources": fake_GetJobRunSources,
    "GetJobRunTests": fake_GetJobRunTests,
    "GetMacroArguments": fake_GetMacroArguments,
}
