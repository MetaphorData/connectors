import datetime
import json
from typing import Optional

from databricks import sql
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import ApiClient
from databricks.sdk.service.sql import EndpointInfo, QueryFilter, TimeRange
from databricks.sql.client import Connection
from requests import HTTPError

from metaphor.common.logger import json_dump_to_debug_file
from metaphor.unity_catalog.config import UnityCatalogQueryLogConfig
from metaphor.unity_catalog.models import ColumnLineage, TableLineage


def list_table_lineage(client: ApiClient, table_name: str) -> TableLineage:
    _data = {"table_name": table_name}
    resp = None

    try:
        resp = client.do(
            "GET", "/api/2.0/lineage-tracking/table-lineage", data=json.dumps(_data)
        )
        json_dump_to_debug_file(resp, f"table-lineage-{table_name}.json")
        return TableLineage.model_validate(resp)
    except HTTPError as e:
        # Lineage API returns 503 on GCP as it's not yet available
        if e.response is not None and e.response.status_code == 503:
            return TableLineage()

        raise e


def list_column_lineage(
    client: ApiClient, table_name: str, column_name: str
) -> ColumnLineage:
    _data = {"table_name": table_name, "column_name": column_name}

    # Lineage API returns 503 on GCP as it's not yet available
    try:
        resp = client.do(
            "GET", "/api/2.0/lineage-tracking/column-lineage", data=json.dumps(_data)
        )
        json_dump_to_debug_file(resp, f"column-lineage-{table_name}-{column_name}.json")
        return ColumnLineage.model_validate(resp)
    except HTTPError as e:
        # Lineage API returns 503 on GCP as it's not yet available
        if e.response is not None and e.response.status_code == 503:
            return ColumnLineage()

        raise e


def build_query_log_filter_by(
    config: UnityCatalogQueryLogConfig,
    client: WorkspaceClient,
) -> QueryFilter:
    end_time = datetime.datetime.now(tz=datetime.timezone.utc)
    start_time = end_time - datetime.timedelta(days=config.lookback_days)

    query_filter = QueryFilter(
        query_start_time_range=TimeRange(
            end_time_ms=int(end_time.timestamp() * 1000),
            start_time_ms=int(start_time.timestamp() * 1000),
        )
    )
    if config.excluded_usernames:
        query_filter.user_ids = [
            user.id
            for user in client.users.list()
            if user.user_name not in config.excluded_usernames
        ]

    return query_filter


SPECIAL_CHARACTERS = "&*{}[],=-()+;'\"`"
"""
The special characters mentioned in Databricks documentation are:
- `
- `-`

The following characters are not allowed:
- `.`
- ` `
- `/`

See https://docs.databricks.com/en/sql/language-manual/sql-ref-names.html for reference.
"""


def escape_special_characters(name: str) -> str:
    """
    When referencing UC names in SQL, you must use backticks to escape
    names that contain special characters such as hyphens (-).
    """
    if any(c in name for c in SPECIAL_CHARACTERS):
        return f"`{name}`"
    return name


def create_connection(
    client: WorkspaceClient, token: str, warehouse_id: Optional[str] = None
) -> Connection:
    endpoints = list(client.warehouses.list())
    if not endpoints:
        raise ValueError("No valid warehouse found")
    endpoint_info: EndpointInfo = endpoints[0]
    if warehouse_id:
        try:
            endpoint_info = client.warehouses.get(warehouse_id)
        except Exception:
            raise ValueError(f"Invalid warehouse id: {warehouse_id}")
    return sql.connect(
        server_hostname=endpoint_info.odbc_params.hostname,
        http_path=endpoint_info.odbc_params.path,
        access_token=token,
    )


def create_api(host: str, token: str) -> WorkspaceClient:
    return WorkspaceClient(host=host, token=token)
