from dataclasses import dataclass
from typing import Dict, List, Optional

from dataclasses_json import dataclass_json

######################
# Models for manifest


@dataclass_json
@dataclass
class ManifestMetadata:
    adapter_type: str


@dataclass_json
@dataclass
class ManifestTestMetadata:
    @dataclass_json
    @dataclass
    class Kwargs:
        column_name: str
        model: str

    name: str
    kwargs: Kwargs


@dataclass_json
@dataclass
class ManifestColumn:
    name: str
    description: Optional[str] = None
    data_type: Optional[str] = None


@dataclass_json
@dataclass
class DbtManifestNode:
    @dataclass_json
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


@dataclass_json
@dataclass
class DbtManifest:
    metadata: ManifestMetadata
    nodes: Dict[str, DbtManifestNode]
    sources: Dict[str, DbtManifestNode]

    @classmethod
    def from_json_file(cls, path: str) -> "DbtManifest":
        with open(path, encoding="utf8") as fin:
            # Ignored due to https://github.com/lidatong/dataclasses-json/issues/23
            return cls.from_json(fin.read())  # type: ignore


######################
# Models for catalog


@dataclass_json
@dataclass
class CatalogStats:
    @dataclass_json
    @dataclass
    class HasStats:
        value: bool

    @dataclass_json
    @dataclass
    class RowCount:
        value: float

    @dataclass_json
    @dataclass
    class Bytes:
        value: float

    @dataclass_json
    @dataclass
    class LastModified:
        value: str

    has_stats: HasStats
    row_count: Optional[RowCount] = None
    bytes: Optional[Bytes] = None
    last_modified: Optional[LastModified] = None


@dataclass_json
@dataclass
class CatalogMetadata:
    database: str
    schema: str
    name: str


@dataclass_json
@dataclass
class CatalogColumn:
    name: str
    comment: Optional[str] = None
    type: Optional[str] = None


@dataclass_json
@dataclass
class DbtCatalogNode:
    metadata: CatalogMetadata
    columns: Dict[str, CatalogColumn]

    stats: Optional[CatalogStats] = None


@dataclass_json
@dataclass
class DbtCatalog:
    nodes: Dict[str, DbtCatalogNode]
    sources: Dict[str, DbtCatalogNode]

    @classmethod
    def from_json_file(cls, path: str) -> "DbtCatalog":
        with open(path, encoding="utf8") as fin:
            # Ignored due to https://github.com/lidatong/dataclasses-json/issues/23
            return cls.from_json(fin.read())  # type: ignore
