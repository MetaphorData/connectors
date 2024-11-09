from typing import Optional

from metaphor.models.metadata_change_event import LogType

# https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata.QueryStatementType
_query_type_map = {
    "SELECT": LogType.SELECT,
    "INSERT": LogType.INSERT,
    "UPDATE": LogType.UPDATE,
    "DELETE": LogType.DELETE,
    "MERGE": LogType.MERGE,
    "CREATE_TABLE": LogType.CREATE_TABLE,
    "CREATE_TABLE_AS_SELECT": LogType.CREATE_TABLE,
    "CREATE_SNAPSHOT_TABLE": LogType.CREATE_TABLE,
    "CREATE_VIEW": LogType.CREATE_VIEW,
    "CREATE_MATERIALIZED_VIEW": LogType.CREATE_VIEW,
    "DROP_TABLE": LogType.DROP_TABLE,
    "DROP_EXTERNAL_TABLE": LogType.DROP_TABLE,
    "DROP_SNAPSHOT_TABLE": LogType.DROP_TABLE,
    "DROP_VIEW": LogType.DROP_VIEW,
    "DROP_MATERIALIZED_VIEW": LogType.DROP_VIEW,
    "ALTER_TABLE": LogType.ALTER_TABLE,
    "ALTER_VIEW": LogType.ALTER_VIEW,
    "ALTER_MATERIALIZED_VIEW": LogType.ALTER_VIEW,
    "TRUNCATE_TABLE": LogType.TRUNCATE,
    "EXPORT_DATA": LogType.EXPORT,
}


def query_type_to_log_type(query_type: Optional[str]) -> Optional[LogType]:
    """
    Converts query type to LogType if it is not None.
    """
    if not query_type:
        return None
    return _query_type_map.get(query_type.upper(), LogType.OTHER)
