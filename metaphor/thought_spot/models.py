"""
This module models the ThoughtSpot SDK complex return objects.
"""
from enum import Enum
from typing import Dict, List, Optional

from pydantic import BaseModel


class Tag(BaseModel):
    name: str
    isDeleted: bool
    isHidden: bool
    isDeprecated: bool


class Header(BaseModel):
    id: str
    name: str
    description: Optional[str]
    tags: List[Tag] = []

    def __repr__(self):
        return self.id


class Reference(BaseModel):
    id: str
    name: str


class Metadata(BaseModel):
    header: Header

    def __repr__(self):
        return self.header.id


class DataSourceConfiguration(BaseModel):
    accountName: Optional[str]
    user: Optional[str]
    project_id: Optional[str]


class DataSourceContent(BaseModel):
    configuration: DataSourceConfiguration


class TableMappingInfo(BaseModel):
    databaseName: str
    schemaName: str
    tableName: str
    tableType: str


class LogicalTableContent(BaseModel):
    physicalTableName: Optional[str]
    worksheetType: str
    joinType: str
    tableMappingInfo: Optional[TableMappingInfo]
    sqlQuery: Optional[str]


class ConnectionType(Enum):
    SNOWFLAKE = "RDBMS_SNOWFLAKE"
    BIGQUERY = "RDBMS_GCP_BIGQUERY"
    REDSHIFT = "RDBMS_REDSHIFT"


class DataSourceTypeEnum(Enum):
    DEFAULT = "DEFAULT"
    SNOWFLAKE = "RDBMS_SNOWFLAKE"
    BIGQUERY = "RDBMS_GCP_BIGQUERY"
    REDSHIFT = "RDBMS_REDSHIFT"


class ColumnSource(BaseModel):
    tableId: str
    columnId: str


class ColumnMappingInfo(BaseModel):
    columnName: str


class ColumnMetadata(Metadata):
    type: str
    columnMappingInfo: Optional[ColumnMappingInfo]
    sources: List[ColumnSource]
    dataType: Optional[str]
    optionalType: Optional[str]


class SourceType(Enum):
    WORKSHEET = "WORKSHEET"
    ONE_TO_ONE_LOGICAL = "ONE_TO_ONE_LOGICAL"
    AGGR_WORKSHEET = "AGGR_WORKSHEET"
    SQL_VIEW = "SQL_VIEW"


class VizColumn(BaseModel):
    referencedTableHeaders: Optional[List[Reference]]
    referencedColumnHeaders: Optional[List[Reference]]


class RefAnswerBook(BaseModel):
    sheets: List[Reference]


class VizContent(BaseModel):
    vizType: str
    chartType: Optional[str]
    refVizId: Optional[str]
    refAnswerBook: Optional[RefAnswerBook]
    columns: List[VizColumn] = []


class Visualization(Metadata):
    vizContent: VizContent


class SheetContent(BaseModel):
    visualizations: List[Visualization] = []


class Question(BaseModel):
    text: str


class Sheet(Metadata):
    sheetContent: SheetContent
    sheetType: Optional[str]
    question: Optional[Question]


class ReportContent(BaseModel):
    sheets: List[Sheet]


class ResolvedObject(Metadata):
    reportContent: ReportContent


class LiveBoardHeader(Header):
    resolvedObjects: Dict[str, ResolvedObject]


class LogicalTable(Metadata):
    columns: List[ColumnMetadata]
    logicalTableContent: LogicalTableContent


class ConnectionDetail(Header):
    type: ConnectionType
    configuration: str


class Connection(BaseModel):
    details: ConnectionDetail


class LogicalTableMetadataDetail(Metadata):
    type: str
    columns: List[ColumnMetadata]
    dataSourceId: str
    dataSourceTypeEnum: DataSourceTypeEnum
    logicalTableContent: LogicalTableContent


class LogicalTableMetadata(BaseModel):
    metadata_detail: LogicalTableMetadataDetail


class AnswerMetadataDetail(Metadata):
    type: str
    reportContent: ReportContent


class AnswerMetadata(BaseModel):
    metadata_detail: AnswerMetadataDetail


class LiveBoardMetadataDetail(Metadata):
    header: LiveBoardHeader
    type: str
    reportContent: ReportContent


class LiveBoardMetadata(BaseModel):
    metadata_detail: LiveBoardMetadataDetail


class TMLResult(BaseModel):
    info: Header
    edoc: Optional[str]


class TMLFormula(BaseModel):
    id: Optional[str]
    name: str
    expr: str


class TMLBase(BaseModel):
    name: str
    formulas: Optional[List[TMLFormula]]


class TMLColumn(BaseModel):
    name: str
    column_id: Optional[str]
    formula_id: Optional[str]


class TMLWorksheet(TMLBase):
    worksheet_columns: List[TMLColumn]


class TMLView(TMLBase):
    view_columns: List[TMLColumn]


class TMLTable(BaseModel):
    name: str
    id: Optional[str]
    fqn: Optional[str]


class TMLAnswerTableObject(BaseModel):
    ordered_column_ids: List[str]


class TMLAnswer(TMLBase):
    tables: List[TMLTable]
    table: TMLAnswerTableObject


class TMLObject(BaseModel):
    guid: str
    worksheet: Optional[TMLWorksheet]
    view: Optional[TMLView]
    answer: Optional[TMLAnswer]
