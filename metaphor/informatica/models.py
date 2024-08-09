import datetime
from typing import List, Optional

from pydantic import BaseModel


class AuthResponseAuthInfo(BaseModel):
    sessionId: str
    status: str


class AuthResponseProduct(BaseModel):
    name: str
    baseApiUrl: str


class AuthResponse(BaseModel):
    """
    doc: https://docs.informatica.com/integration-cloud/b2b-gateway/current-version/rest-api-reference/platform_rest_api_version_3_resources/login_2.html
    """

    userInfo: AuthResponseAuthInfo
    products: List[AuthResponseProduct]


class ConnectorParams(BaseModel):
    account: Optional[str] = None


class ConnectionDetail(BaseModel):
    """
    doc: https://docs.informatica.com/integration-cloud/b2b-gateway/current-version/rest-api-reference/data_integration_rest_api/connection.html
    """

    id: str
    connParams: Optional[ConnectorParams] = None
    type: str
    connectorGuid: Optional[str] = None
    host: Optional[str] = None
    database: Optional[str] = None


class ObjectDetail(BaseModel):
    id: str
    tags: List[str]
    path: str


class ObjectDetailResponse(BaseModel):
    """
    doc: https://docs.informatica.com/integration-cloud/b2b-gateway/current-version/rest-api-reference/platform_rest_api_version_3_resources/objects/finding_an_asset.html
    """

    count: int
    objects: List[ObjectDetail]


class ReferenceObjectDetail(BaseModel):
    id: str
    appContextId: Optional[str] = None
    documentType: str


class ObjectReferenceResponse(BaseModel):
    """
    doc: https://docs.informatica.com/integration-cloud/b2b-gateway/current-version/rest-api-reference/platform_rest_api_version_3_resources/objects/finding_asset_dependencies.html
    """

    count: int
    references: List[ReferenceObjectDetail]


class ExtendedObjectDetail(BaseModel):
    name: str


class ExtendedObject(BaseModel):
    object: Optional[ExtendedObjectDetail] = None
    singleMode: bool


class MappingParameter(BaseModel):
    type: str
    extendedObject: Optional[ExtendedObject] = None
    sourceConnectionId: Optional[str] = None
    targetConnectionId: Optional[str] = None
    targetObject: Optional[str] = None
    customQuery: Optional[str] = None


class MappingDetailResponse(BaseModel):
    """
    doc: https://docs.informatica.com/integration-cloud/b2b-gateway/current-version/rest-api-reference/data_integration_rest_api/mapping.html
    """

    name: str
    description: str
    createTime: datetime.datetime
    updateTime: datetime.datetime
    createdBy: str
    updatedBy: str

    parameters: List[MappingParameter]
