from datetime import datetime
from enum import Enum
from typing import Any, List, Optional

from pydantic import BaseModel


class PowerBIApp(BaseModel):
    id: str
    name: str
    workspaceId: str


class PowerBIDataSource(BaseModel):
    datasourceType: str
    datasourceId: str
    connectionDetails: Any = None
    gatewayId: str


class PowerBIDataset(BaseModel):
    id: str
    name: str
    isRefreshable: bool
    webUrl: Optional[str] = None


class PowerBIDatasetParameter(BaseModel):
    name: str
    type: str
    currentValue: str
    isRequired: bool = True
    suggestedValues: List[str] = []


class PowerBIDashboard(BaseModel):
    id: str
    displayName: str
    webUrl: Optional[str] = None


class PowerBIWorkspace(BaseModel):
    id: str
    name: str
    isReadOnly: bool
    type: str


class PowerBIReport(BaseModel):
    id: str
    name: str
    datasetId: Optional[str] = None
    reportType: str
    webUrl: Optional[str] = None


class PowerBIPage(BaseModel):
    name: str
    displayName: str
    order: int


class PowerBITile(BaseModel):
    id: str
    title: str = ""
    datasetId: str = ""
    reportId: str = ""
    embedUrl: Optional[str] = None


class PowerBIRefresh(BaseModel):
    status: str = ""
    endTime: str = ""


class PowerBITableColumn(BaseModel):
    name: str
    dataType: str = "unknown"


class PowerBITableMeasure(BaseModel):
    name: str
    description: Optional[str] = None
    expression: str = ""


class PowerBITable(BaseModel):
    name: str
    columns: List[PowerBITableColumn] = []
    measures: List[PowerBITableMeasure] = []
    source: List[Any] = []


class EndorsementDetails(BaseModel):
    endorsement: str
    certifiedBy: Optional[str] = ""


class UpstreamDataflow(BaseModel):
    targetDataflowId: str


class DataflowTransaction(BaseModel):
    id: str
    status: Optional[str] = None
    startTime: Optional[str] = None
    endTime: Optional[str] = None
    refreshType: Optional[str] = None


class SensitivityLabel(BaseModel):
    labelId: str


class WorkspaceInfoDataset(BaseModel):
    id: str
    name: str
    tables: List[PowerBITable] = []

    description: str = ""
    contentProviderType: str = ""
    createdDate: str = ""
    configuredBy: Optional[str] = None

    upstreamDataflows: Optional[List[UpstreamDataflow]] = None
    upstreamDatasets: Optional[Any] = None
    endorsementDetails: Optional[EndorsementDetails] = None
    sensitivityLabel: Optional[SensitivityLabel] = None


class WorkspaceInfoDashboardBase(BaseModel):
    id: str
    appId: Optional[str] = None
    createdDateTime: Optional[str] = None
    modifiedDateTime: Optional[str] = None
    createdBy: Optional[str] = None
    modifiedBy: Optional[str] = None
    endorsementDetails: Optional[EndorsementDetails] = None
    sensitivityLabel: Optional[SensitivityLabel] = None


class WorkspaceInfoDashboard(WorkspaceInfoDashboardBase):
    displayName: str


class WorkspaceInfoReport(WorkspaceInfoDashboardBase):
    name: str
    datasetId: Optional[str] = None
    description: str = ""
    sensitivityLabel: Optional[SensitivityLabel] = None


class WorkspaceInfoUser(BaseModel):
    emailAddress: Optional[str] = None
    groupUserAccessRight: str
    displayName: Optional[str] = None
    graphId: str
    principalType: str

    def __hash__(self):
        return hash(self.graphId)


class PowerBiRefreshSchedule(BaseModel):
    frequency: Optional[int] = None
    days: Optional[List[str]] = None
    times: Optional[List[str]] = None
    enabled: Optional[bool] = None
    localTimeZoneId: Optional[str] = None
    notifyOption: Optional[str] = None


class WorkspaceInfoDataflow(BaseModel):
    objectId: str
    name: Optional[str] = None
    description: Optional[str] = None
    configuredBy: Optional[str] = None
    modifiedBy: Optional[str] = None
    modifiedDateTime: Optional[str] = None
    refreshSchedule: Optional[PowerBiRefreshSchedule] = None


class WorkspaceInfo(BaseModel):
    id: str
    name: Optional[str] = None
    type: Optional[str] = None
    state: str
    description: Optional[str] = None
    reports: List[WorkspaceInfoReport] = []
    datasets: List[WorkspaceInfoDataset] = []
    dashboards: List[WorkspaceInfoDashboard] = []
    dataflows: List[WorkspaceInfoDataflow] = []
    users: Optional[List[WorkspaceInfoUser]] = []


class PowerBiSubscriptionUser(BaseModel):
    emailAddress: str
    displayName: str


class PowerBISubscription(BaseModel):
    id: str
    artifactId: str
    title: Optional[str] = None
    frequency: Optional[str] = None
    endDate: Optional[str] = None
    startDate: Optional[str] = None
    artifactDisplayName: Optional[str] = None
    subArtifactDisplayName: Optional[str] = None
    users: List[PowerBiSubscriptionUser] = []


class SubscriptionsByUserResponse(BaseModel):
    SubscriptionEntities: List[PowerBISubscription]
    continuationUri: Optional[str] = None


class PowerBIActivityEventEntity(BaseModel):
    Id: str
    CreationTime: datetime
    OrganizationId: str
    WorkspaceId: Optional[str] = None
    UserType: int
    UserId: str
    Activity: str
    IsSuccess: Optional[bool] = None
    RequestId: Optional[str] = None

    ArtifactKind: Optional[str] = None
    ArtifactId: Optional[str] = None


class PowerBIActivityType(Enum):
    view_report = "ViewReport"
    view_dashboard = "ViewDashboard"
    view_metadata = "ViewMetadata"
    view_dataflow = "ViewDataflow"
    view_tile = "ViewTile"


class GetActivitiesResponse(BaseModel):
    activityEventEntities: List[PowerBIActivityEventEntity]
    continuationUri: Optional[str] = None
