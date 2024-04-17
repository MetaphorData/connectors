import re
from datetime import datetime
from typing import Dict, List, Optional
from urllib import parse

from metaphor.common.logger import get_logger
from metaphor.common.utils import safe_parse_ISO8601
from metaphor.models.metadata_change_event import Chart, ChartType
from metaphor.models.metadata_change_event import PowerBIApp as PbiApp
from metaphor.models.metadata_change_event import PowerBIColumn as PbiColumn
from metaphor.models.metadata_change_event import (
    PowerBIDashboardType,
    PowerBIDatasetTable,
    PowerBIEndorsement,
    PowerBIEndorsementType,
    PowerBIInfo,
)
from metaphor.models.metadata_change_event import PowerBIMeasure as PbiMeasure
from metaphor.models.metadata_change_event import PowerBIRefreshSchedule
from metaphor.power_bi.models import (
    DataflowTransaction,
    PowerBIApp,
    PowerBIPage,
    PowerBIRefresh,
    PowerBITile,
    WorkspaceInfo,
    WorkspaceInfoDashboardBase,
    WorkspaceInfoDataset,
)
from metaphor.power_bi.power_bi_client import PowerBIClient

logger = get_logger()


def get_pbi_dataset_tables(wds: WorkspaceInfoDataset) -> List[PowerBIDatasetTable]:
    return [
        PowerBIDatasetTable(
            columns=[PbiColumn(field=c.name, type=c.dataType) for c in table.columns],
            measures=[
                PbiMeasure(
                    field=m.name,
                    expression=m.expression,
                    description=m.description,
                )
                for m in table.measures
            ],
            name=table.name,
            expression=(
                table.source[0].get("expression", None)
                if len(table.source) > 0
                else None
            ),
        )
        for table in wds.tables
    ]


def extract_refresh_schedule(
    client: PowerBIClient, workspace_id: str, dataset_id: str
) -> Optional[PowerBIRefreshSchedule]:
    modeled_dataset_refresh_schedule = client.get_refresh_schedule(
        workspace_id, dataset_id
    )

    if modeled_dataset_refresh_schedule:
        return PowerBIRefreshSchedule(
            days=modeled_dataset_refresh_schedule.days,
            times=modeled_dataset_refresh_schedule.times,
            enabled=modeled_dataset_refresh_schedule.enabled or False,
            local_time_zone_id=modeled_dataset_refresh_schedule.localTimeZoneId,
            notify_option=modeled_dataset_refresh_schedule.notifyOption,
        )

    direct_query_dataset_refresh_schedule = client.get_direct_query_refresh_schedule(
        workspace_id, dataset_id
    )

    if direct_query_dataset_refresh_schedule:
        if direct_query_dataset_refresh_schedule.frequency:
            frequency_in_minutes = float(
                direct_query_dataset_refresh_schedule.frequency
            )
        else:
            frequency_in_minutes = None
        return PowerBIRefreshSchedule(
            frequency_in_minutes=frequency_in_minutes,
            days=direct_query_dataset_refresh_schedule.days,
            times=direct_query_dataset_refresh_schedule.times,
            enabled=direct_query_dataset_refresh_schedule.enabled or False,
            local_time_zone_id=direct_query_dataset_refresh_schedule.localTimeZoneId,
            notify_option=direct_query_dataset_refresh_schedule.notifyOption,
        )

    return None


def make_power_bi_info(
    type: PowerBIDashboardType,
    workspace: WorkspaceInfo,
    dashboard: WorkspaceInfoDashboardBase,
    app_map: Dict[str, PowerBIApp],
) -> PowerBIInfo:
    pbi_info = PowerBIInfo(
        power_bi_dashboard_type=type,
        workspace_id=workspace.id,
        created_by=dashboard.createdBy,
        created_date_time=safe_parse_ISO8601(dashboard.createdDateTime),
        modified_by=dashboard.modifiedBy,
        modified_date_time=safe_parse_ISO8601(dashboard.modifiedDateTime),
    )

    if dashboard.appId is not None:
        app_id = dashboard.appId
        app = app_map.get(app_id)
        if app is not None:
            pbi_info.app = PbiApp(id=app.id, name=app.name)

    if dashboard.endorsementDetails is not None:
        try:
            endorsement = PowerBIEndorsementType(
                dashboard.endorsementDetails.endorsement
            )
            pbi_info.endorsement = PowerBIEndorsement(
                endorsement=endorsement,
                certified_by=dashboard.endorsementDetails.certifiedBy,
            )
        except ValueError:
            logger.warning(
                f"Endorsement type {dashboard.endorsementDetails.endorsement} are not supported"
            )

    return pbi_info


def get_workspace_hierarchy(workspace: WorkspaceInfo) -> List[str]:
    return (workspace.name or "").split(".")


def find_last_completed_refresh(
    refreshes: List[PowerBIRefresh],
) -> Optional[datetime]:
    try:
        # Assume refreshes are already sorted in reversed chronological order
        # Empty endTime means still in progress
        refresh = next(
            r for r in refreshes if r.status == "Completed" and r.endTime != ""
        )
    except StopIteration:
        return None

    return safe_parse_ISO8601(refresh.endTime)


def find_refresh_time_from_transaction(
    transactions: List[DataflowTransaction],
) -> Optional[datetime]:
    """
    Find the last success transaction (refresh) time from a list of dataflow transactions
    """
    try:
        # Assume refreshes are already sorted in reversed chronological order
        # Empty endTime means still in progress
        refresh = next(
            t for t in transactions if t.status == "Success" and t.endTime != ""
        )
    except StopIteration:
        return None

    return safe_parse_ISO8601(refresh.endTime)


def get_dashboard_id_from_url(url: str) -> Optional[str]:
    path = parse.urlparse(url).path
    pattern = re.compile(r"apps/([^/]+)/(reports|dashboards)/([^/]+)")
    match = pattern.search(path)
    if match and len(match.groups()) == 3:
        return match.group(3)
    return None


def transform_tiles_to_charts(tiles: List[PowerBITile]) -> List[Chart]:
    return [
        Chart(title=t.title, url=t.embedUrl, chart_type=ChartType.OTHER) for t in tiles
    ]


def transform_pages_to_charts(pages: List[PowerBIPage]) -> List[Chart]:
    return [Chart(title=p.displayName, chart_type=ChartType.OTHER) for p in pages]
