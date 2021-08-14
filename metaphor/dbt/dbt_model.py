from dataclasses import dataclass
from typing import Dict, List, Optional

from serde import deserialize
from serde.json import from_json

######################
# Models for manifest


@deserialize
@dataclass
class ManifestMetadata:
    adapter_type: str


@deserialize
@dataclass
class ManifestTestMetadata:
    @deserialize
    @dataclass
    class Kwargs:
        column_name: str
        model: str

    name: str
    kwargs: Kwargs


@deserialize
@dataclass
class ManifestColumn:
    name: str
    description: Optional[str] = None
    data_type: Optional[str] = None


@deserialize
@dataclass
class DbtManifestNode:
    @deserialize
    @dataclass
    class DependsOn:
        macros: List[str]
        nodes: List[str]

    resource_type: str
    original_file_path: str
    columns: Dict[str, ManifestColumn]
    database: str
    schema: str
    name: str

    compiled_sql: Optional[str] = None
    description: Optional[str] = None
    depends_on: Optional[DependsOn] = None
    test_metadata: Optional[ManifestTestMetadata] = None


@deserialize
@dataclass
class DbtManifest:
    metadata: ManifestMetadata
    nodes: Dict[str, DbtManifestNode]
    sources: Dict[str, DbtManifestNode]

    @classmethod
    def from_json_file(cls, path: str) -> "DbtManifest":
        with open(path, encoding="utf8") as fin:
            # Ignored due to https://github.com/lidatong/dataclasses-json/issues/23
            return from_json(cls, fin.read())  # type: ignore


######################
# Models for catalog


@deserialize
@dataclass
class CatalogStats:
    @deserialize
    @dataclass
    class HasStats:
        value: bool

    @deserialize
    @dataclass
    class RowCount:
        value: float

    @deserialize
    @dataclass
    class Bytes:
        value: float

    @deserialize
    @dataclass
    class LastModified:
        value: str

    has_stats: HasStats
    row_count: Optional[RowCount] = None
    bytes: Optional[Bytes] = None
    last_modified: Optional[LastModified] = None


@deserialize
@dataclass
class CatalogMetadata:
    database: str
    schema: str
    name: str


@deserialize
@dataclass
class CatalogColumn:
    name: str
    comment: Optional[str] = None
    type: Optional[str] = None


@deserialize
@dataclass
class DbtCatalogNode:
    metadata: CatalogMetadata
    columns: Dict[str, CatalogColumn]

    stats: Optional[CatalogStats] = None


@deserialize
@dataclass
class DbtCatalog:
    nodes: Dict[str, DbtCatalogNode]
    sources: Dict[str, DbtCatalogNode]

    @classmethod
    def from_json_file(cls, path: str) -> "DbtCatalog":
        with open(path, encoding="utf8") as fin:
            # Ignored due to https://github.com/lidatong/dataclasses-json/issues/23
            return from_json(cls, fin.read())  # type: ignore
