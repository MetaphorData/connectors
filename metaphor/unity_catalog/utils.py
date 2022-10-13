from databricks_cli.sdk.api_client import ApiClient
from pydantic import parse_obj_as

from metaphor.unity_catalog.models import ColumnLineage, TableLineage


def list_table_lineage(client: ApiClient, table_name: str) -> TableLineage:
    _data = {"table_name": table_name}
    return parse_obj_as(
        TableLineage,
        client.perform_query(
            "GET", "/lineage-tracking/table-lineage", data=_data, version="2.0"
        ),
    )


def list_column_lineage(
    client: ApiClient, table_name: str, column_name: str
) -> ColumnLineage:
    _data = {"table_name": table_name, "column_name": column_name}
    return parse_obj_as(
        ColumnLineage,
        client.perform_query(
            "GET", "/lineage-tracking/column-lineage", data=_data, version="2.0"
        ),
    )
