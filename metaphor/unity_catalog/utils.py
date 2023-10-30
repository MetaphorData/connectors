from databricks.sdk.core import ApiClient
from pydantic import parse_obj_as
from requests import HTTPError

from metaphor.common.logger import json_dump_to_debug_file
from metaphor.unity_catalog.models import ColumnLineage, TableLineage


def list_table_lineage(client: ApiClient, table_name: str) -> TableLineage:
    _data = {"table_name": table_name}
    resp = None

    try:
        resp = client.do("GET", "/api/2.0/lineage-tracking/table-lineage", data=_data)
        json_dump_to_debug_file(resp, f"table-lineage-{table_name}.json")
        return parse_obj_as(TableLineage, resp)
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
        resp = client.do("GET", "/api/2.0/lineage-tracking/column-lineage", data=_data)
        json_dump_to_debug_file(resp, f"column-lineage-{table_name}-{column_name}.json")
        return parse_obj_as(ColumnLineage, resp)
    except HTTPError as e:
        # Lineage API returns 503 on GCP as it's not yet available
        if e.response is not None and e.response.status_code == 503:
            return ColumnLineage()

        raise e
