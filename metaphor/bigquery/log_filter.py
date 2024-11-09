from typing import Literal

from metaphor.bigquery.config import BigQueryRunConfig
from metaphor.bigquery.queries import Queries
from metaphor.common.utils import start_of_day


def build_list_entries_filter(
    target: Literal["audit_log", "query_log"],
    config: BigQueryRunConfig,
) -> str:
    """
    Builds the filter for a list entries query.
    """
    start_time = start_of_day(
        config.query_log.lookback_days
        if target == "query_log"
        else config.lineage.lookback_days
    ).isoformat()
    end_time = start_of_day().isoformat()

    # Filter for service account
    service_account_filter = (
        "NOT protoPayload.authenticationInfo.principalEmail:gserviceaccount.com AND"
        if target == "query_log" and config.query_log.exclude_service_accounts
        else ""
    )

    # Filter for destination table
    destination_table_filter = (
        'NOT protoPayload.metadata.jobChange.job.jobConfig.queryConfig.destinationTable:"/datasets/_" AND'
        if target == "audit_log"
        else ""
    )

    return Queries.log_query_filter(
        service_account_filter, destination_table_filter, start_time, end_time
    )
