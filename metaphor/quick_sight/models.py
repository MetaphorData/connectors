from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Union

from pydantic.dataclasses import dataclass


class TypeImportMode(Enum):
    DIRECT_QUERY = "DIRECT_QUERY"
    SPICE = "SPICE"


@dataclass
class DataSetColumn:
    Description: Optional[str] = None
    Name: Optional[str] = None
    Type: Optional[str] = None


@dataclass
class TypeProjectOperation:
    ProjectedColumns: List[str]


@dataclass
class CalculatedColumn:
    ColumnName: str
    ColumnId: str
    Expression: str


@dataclass
class TypeCreateColumnsOperation:
    Columns: List[CalculatedColumn]


@dataclass
class TypeRenameColumnOperation:
    ColumnName: str
    NewColumnName: str


@dataclass
class TransformOperation:
    CreateColumnsOperation: Optional[TypeCreateColumnsOperation] = None
    ProjectOperation: Optional[TypeProjectOperation] = None
    RenameColumnOperation: Optional[TypeRenameColumnOperation] = None


@dataclass
class TypeJoinInstruction:
    LeftOperand: str
    OnClause: str
    RightOperand: str
    Type: str


@dataclass
class DataSetSource:
    DataSetArn: Optional[str] = None
    JoinInstruction: Optional[TypeJoinInstruction] = None
    PhysicalTableId: Optional[str] = None


@dataclass
class DataSetLogicalTable:
    Alias: str
    Source: DataSetSource
    DataTransforms: Optional[List[TransformOperation]] = None


@dataclass
class TypeRelationalTable:
    DataSourceArn: str
    InputColumns: List[DataSetColumn]
    Name: str
    Catalog: Optional[str] = None
    Schema: Optional[str] = None


@dataclass
class TypeCustomSql:
    DataSourceArn: str
    Name: str
    SqlQuery: str
    Columns: List[DataSetColumn]


@dataclass
class TypeS3Source:
    DataSourceArn: str
    InputColumns: List[DataSetColumn]


@dataclass
class DataSetPhysicalTable:
    CustomSql: Optional[TypeCustomSql] = None
    RelationalTable: Optional[TypeRelationalTable] = None
    S3Source: Optional[TypeS3Source] = None


@dataclass
class DataSet:
    """
    @see: https://docs.aws.amazon.com/quicksight/latest/APIReference/API_DataSet.html
    """

    Arn: Optional[str] = None
    CreatedTime: Optional[datetime] = None
    DataSetId: Optional[str] = None
    ImportMode: Optional[TypeImportMode] = None
    LastUpdatedTime: Optional[datetime] = None
    LogicalTableMap: Optional[Dict[str, DataSetLogicalTable]] = None
    Name: Optional[str] = None
    OutputColumns: Optional[List[DataSetColumn]] = None
    PhysicalTableMap: Optional[Dict[str, DataSetPhysicalTable]] = None


@dataclass
class Sheet:
    Name: Optional[str] = None
    SheetId: Optional[str] = None


@dataclass
class VersionedDashboard:
    Arn: Optional[str] = None
    CreatedTime: Optional[datetime] = None
    DataSetArns: Optional[List[str]] = None
    Description: Optional[str] = None
    Sheets: Optional[List[Sheet]] = None
    SourceEntityArn: Optional[str] = None
    VersionNumber: Optional[int] = None


@dataclass
class Dashboard:
    """
    @see: https://docs.aws.amazon.com/quicksight/latest/APIReference/API_Dashboard.html
    """

    Arn: Optional[str] = None
    CreatedTime: Optional[datetime] = None
    DashboardId: Optional[str] = None
    LastPublishedTime: Optional[datetime] = None
    LastUpdatedTime: Optional[datetime] = None
    Name: Optional[str] = None
    Version: Optional[VersionedDashboard] = None


@dataclass
class Visual:
    BarChartVisual: Optional[Any] = None
    BoxPlotVisual: Optional[Any] = None
    ComboChartVisual: Optional[Any] = None
    CustomContentVisual: Optional[Any] = None
    EmptyVisual: Optional[Any] = None
    FilledMapVisual: Optional[Any] = None
    FunnelChartVisual: Optional[Any] = None
    GaugeChartVisual: Optional[Any] = None
    GeospatialMapVisual: Optional[Any] = None
    HeatMapVisual: Optional[Any] = None
    HistogramVisual: Optional[Any] = None
    InsightVisual: Optional[Any] = None
    KPIVisual: Optional[Any] = None
    LineChartVisual: Optional[Any] = None
    PieChartVisual: Optional[Any] = None
    PivotTableVisual: Optional[Any] = None
    RadarChartVisual: Optional[Any] = None
    SankeyDiagramVisual: Optional[Any] = None
    ScatterPlotVisual: Optional[Any] = None
    TreeMapVisual: Optional[Any] = None
    WaterfallVisual: Optional[Any] = None
    WordCloudVisual: Optional[Any] = None


@dataclass
class SheetDefinition:
    SheetId: str
    ContentType: Optional[str] = None
    Description: Optional[str] = None
    Name: Optional[str] = None
    Title: Optional[str] = None
    Visuals: Optional[List[Visual]] = None


@dataclass
class DataSetIdentifierDeclaration:
    DataSetArn: str
    Identifier: str


@dataclass
class AnalysisDefinition:
    """
    @see: https://docs.aws.amazon.com/quicksight/latest/APIReference/API_AnalysisDefinition.html
    """

    DataSetIdentifierDeclarations: List[DataSetIdentifierDeclaration]
    Sheets: Optional[List[SheetDefinition]] = None


@dataclass
class Analysis:
    AnalysisId: str
    Arn: str
    Name: str
    Definition: AnalysisDefinition


class DataSourceType(Enum):
    """
    @see: https://docs.aws.amazon.com/quicksight/latest/APIReference/API_DataSource.html
    """

    ADOBE_ANALYTICS = "ADOBE_ANALYTICS"
    AMAZON_ELASTICSEARCH = "AMAZON_ELASTICSEARCH"
    ATHENA = "ATHENA"
    AURORA = "AURORA"
    AURORA_POSTGRESQL = "AURORA_POSTGRESQL"
    AWS_IOT_ANALYTICS = "AWS_IOT_ANALYTICS"
    GITHUB = "GITHUB"
    JIRA = "JIRA"
    MARIADB = "MARIADB"
    MYSQL = "MYSQL"
    ORACLE = "ORACLE"
    POSTGRESQL = "POSTGRESQL"
    PRESTO = "PRESTO"
    REDSHIFT = "REDSHIFT"
    S3 = "S3"
    SALESFORCE = "SALESFORCE"
    SERVICENOW = "SERVICENOW"
    SNOWFLAKE = "SNOWFLAKE"
    SPARK = "SPARK"
    SQLSERVER = "SQLSERVER"
    TERADATA = "TERADATA"
    TWITTER = "TWITTER"
    TIMESTREAM = "TIMESTREAM"
    AMAZON_OPENSEARCH = "AMAZON_OPENSEARCH"
    EXASOL = "EXASOL"
    DATABRICKS = "DATABRICKS"
    STARBURST = "STARBURST"
    TRINO = "TRINO"
    BIGQUERY = "BIGQUERY"


@dataclass
class DatabaseParameters:
    Database: str
    Host: str
    Port: int


@dataclass
class TypeAuroraParameters(DatabaseParameters):
    pass


@dataclass
class TypeAuroraPostgreSqlParameters(DatabaseParameters):
    pass


@dataclass
class TypeBigQueryParameters:
    ProjectId: str


@dataclass
class TypeDatabricksParameters:
    Host: str
    Port: int
    SqlEndpointPath: str


@dataclass
class TypeMariaDbParameters(DatabaseParameters):
    pass


@dataclass
class TypeMySqlParameters(DatabaseParameters):
    pass


@dataclass
class TypeOracleParameters(DatabaseParameters):
    pass


@dataclass
class TypePostgreSqlParameters(DatabaseParameters):
    pass


@dataclass
class TypeRdsParameters:
    Database: str
    InstanceId: str


@dataclass
class TypeRedshiftParameters:
    Database: str
    ClusterId: Optional[str] = None
    Host: Optional[str] = None
    Port: Optional[int] = None


@dataclass
class TypeSnowflakeParameters:
    Database: str
    Host: str


@dataclass
class TypeSqlServerParameters(DatabaseParameters):
    pass


@dataclass
class TypeDataSourceParameters:
    AuroraParameters: Optional[TypeAuroraParameters] = None
    AuroraPostgreSqlParameters: Optional[TypeAuroraPostgreSqlParameters] = None
    BigQueryParameters: Optional[TypeBigQueryParameters] = None
    DatabricksParameters: Optional[TypeDatabricksParameters] = None
    MariaDbParameters: Optional[TypeMariaDbParameters] = None
    MySqlParameters: Optional[TypeMySqlParameters] = None
    OracleParameters: Optional[TypeOracleParameters] = None
    PostgreSqlParameters: Optional[TypePostgreSqlParameters] = None
    RdsParameters: Optional[TypeRdsParameters] = None
    RedshiftParameters: Optional[TypeRedshiftParameters] = None
    SnowflakeParameters: Optional[TypeSnowflakeParameters] = None
    SqlServerParameters: Optional[TypeSqlServerParameters] = None


@dataclass
class DataSource:
    Arn: Optional[str] = None
    CreatedTime: Optional[datetime] = None
    DataSourceId: Optional[str] = None
    Name: Optional[str] = None
    Type: Optional[DataSourceType] = None
    DataSourceParameters: Optional[TypeDataSourceParameters] = None


@dataclass
class Folder:
    """
    @see: https://docs.aws.amazon.com/quicksight/latest/APIReference/API_Folder.html
    """

    Arn: Optional[str] = None
    CreatedTime: Optional[datetime] = None
    FolderId: Optional[str] = None
    FolderPath: Optional[List[str]] = None
    FolderType: Optional[str] = None
    LastUpdatedTime: Optional[datetime] = None
    Name: Optional[str] = None
    SharingModel: Optional[str] = None


@dataclass
class User:
    """
    @see: https://docs.aws.amazon.com/quicksight/latest/APIReference/API_User.html
    """

    Arn: Optional[str] = None
    Email: Optional[str] = None
    PrincipalId: Optional[str] = None
    Role: Optional[str] = None
    UserName: Optional[str] = None


ResourceType = Union[Analysis, DataSet, Dashboard, DataSource, Folder, User]
