from typing import Dict, Optional

from azure.identity import ClientSecretCredential
from msgraph_beta import GraphServiceClient
from msgraph_beta.generated.models.o_data_errors.o_data_error import ODataError

from metaphor.common.logger import get_logger
from metaphor.models.metadata_change_event import PowerBISensitivityLabel
from metaphor.power_bi.config import PowerBIRunConfig

logger = get_logger()


class GraphApiClient:
    def __init__(self, config: PowerBIRunConfig):
        self._client = self.create_graph_client(config)
        self._sensitivity_labels: Dict[str, PowerBISensitivityLabel] = {}

    def create_graph_client(self, config: PowerBIRunConfig) -> GraphServiceClient:
        credential = ClientSecretCredential(
            tenant_id=config.tenant_id,
            client_id=config.client_id,
            client_secret=config.secret,
        )
        scopes = ["https://graph.microsoft.com/.default"]

        return GraphServiceClient(credentials=credential, scopes=scopes)

    async def get_labels(
        self, label_id: Optional[str] = None
    ) -> Optional[PowerBISensitivityLabel]:
        if label_id is None:
            return None

        if label_id in self._sensitivity_labels:
            return self._sensitivity_labels.get(label_id)

        try:
            # This endpoint is still under beta
            # https://learn.microsoft.com/en-us/graph/api/security-informationprotection-list-sensitivitylabels?view=graph-rest-beta&tabs=http
            builder = self._client.security.information_protection.sensitivity_labels.by_sensitivity_label_id(
                label_id
            )
            label = await builder.get()
        except ODataError as error:
            if error.response_status_code == 403:
                logger.error(
                    "Please add InformationProtectionPolicy.Read.All permission to this application"
                )
                return None
            raise error

        if label is None:
            return None

        self._sensitivity_labels[label_id] = PowerBISensitivityLabel(
            description=label.tooltip,
            name=label.name,
            id=label_id,
        )

        return self._sensitivity_labels.get(label_id)
