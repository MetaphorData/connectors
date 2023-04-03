from databricks_cli.sdk.api_client import ApiClient
from pydantic import parse_obj_as
from requests import HTTPError

from metaphor.common.logger import get_logger
from metaphor.unity_catalog.models import ColumnLineage, TableLineage

logger = get_logger()


def list_table_lineage(client: ApiClient, table_name: str) -> TableLineage:
    _data = {"table_name": table_name}

    try:
        return parse_obj_as(
            TableLineage,
            client.perform_query(
                "GET", "/lineage-tracking/table-lineage", data=_data, version="2.0"
            ),
        )
    except HTTPError as e:
        # Lineage API returns 503 on GCP as it's not yet available
        if e.response.status_code == 503:
            return TableLineage()

        raise e


def list_column_lineage(
    client: ApiClient, table_name: str, column_name: str
) -> ColumnLineage:
    _data = {"table_name": table_name, "column_name": column_name}

    # Lineage API returns 503 on GCP as it's not yet available
    try:
        return parse_obj_as(
            ColumnLineage,
            client.perform_query(
                "GET", "/lineage-tracking/column-lineage", data=_data, version="2.0"
            ),
        )
    except HTTPError as e:
        # Lineage API returns 503 on GCP as it's not yet available
        if e.response.status_code == 503:
            return ColumnLineage()

        raise e
