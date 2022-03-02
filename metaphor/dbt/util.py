import re
from email.utils import parseaddr
from typing import Dict, List, Optional, Union

from metaphor.models.metadata_change_event import (
    DataPlatform,
    Dataset,
    DatasetDocumentation,
    DatasetLogicalID,
    DatasetSchema,
    DbtTest,
    FieldDocumentation,
    Metric,
    MetricLogicalID,
    MetricType,
    SchemaField,
    SchemaType,
    VirtualView,
    VirtualViewLogicalID,
    VirtualViewType,
)
from pydantic.utils import unique_list

from metaphor.common.entity_id import (
    EntityId,
    dataset_fullname,
    to_dataset_entity_id,
    to_person_entity_id,
    to_virtual_view_entity_id,
)
from metaphor.dbt.generated.dbt_manifest_v3 import (
    CompiledModelNode as CompiledModelNode_v3,
)
from metaphor.dbt.generated.dbt_manifest_v3 import ParsedModelNode as ParsedModelNode_v3
from metaphor.dbt.generated.dbt_manifest_v4 import (
    CompiledModelNode as CompiledModelNode_v4,
)
from metaphor.dbt.generated.dbt_manifest_v4 import ParsedModelNode as ParsedModelNode_v4

MODEL_NODE_TYPE = Union[
    CompiledModelNode_v3, ParsedModelNode_v3, CompiledModelNode_v4, ParsedModelNode_v4
]


def get_dataset_entity_id(self, db: str, schema: str, table: str) -> EntityId:
    return to_dataset_entity_id(
        dataset_fullname(db, schema, table), self._platform, self._account
    )


def get_virtual_view_id(logical_id: VirtualViewLogicalID) -> str:
    return str(to_virtual_view_entity_id(logical_id.name, logical_id.type))


def get_model_name_from_unique_id(unique_id: str) -> str:
    assert unique_id.startswith("model."), f"invalid model id {unique_id}"
    return unique_id[6:]


def get_metric_name_from_unique_id(unique_id: str) -> str:
    assert unique_id.startswith("metric."), f"invalid metric id {unique_id}"
    return unique_id[7:]


def get_model_owner_ids(
    model: MODEL_NODE_TYPE, owner_key="owner"
) -> Optional[List[str]]:
    # v3 use 'model.config.meta' while v1, v2 use 'model.meta'
    if model.config and model.config.meta and owner_key in model.config.meta:
        owners = model.config.meta[owner_key]
    elif model.meta and owner_key in model.meta:
        owners = model.meta[owner_key]
    else:
        return None

    parts = re.split(r"(\s|,)", owners.strip())
    return unique_list(
        [str(to_person_entity_id(p)) for p in parts if "@" in parseaddr(p)[1]]
    )


def init_dataset(
    datasets: Dict[str, Dataset],
    database: str,
    schema: str,
    name: str,
    platform: DataPlatform,
    account: Optional[str],
    unique_id: str,
) -> Dataset:
    if unique_id not in datasets:
        datasets[unique_id] = Dataset(
            logical_id=DatasetLogicalID(
                name=dataset_fullname(database, schema, name),
                platform=platform,
                account=account,
            )
        )
    return datasets[unique_id]


def init_virtual_view(
    virtual_views: Dict[str, VirtualView], unique_id: str
) -> VirtualView:
    if unique_id not in virtual_views:
        virtual_views[unique_id] = VirtualView(
            logical_id=VirtualViewLogicalID(
                name=get_model_name_from_unique_id(unique_id),
                type=VirtualViewType.DBT_MODEL,
            ),
        )
    return virtual_views[unique_id]


def init_dbt_tests(
    virtual_views: Dict[str, VirtualView], dbt_model_unique_id: str
) -> List[DbtTest]:
    assert dbt_model_unique_id in virtual_views

    dbt_model = virtual_views[dbt_model_unique_id].dbt_model
    if dbt_model.tests is None:
        dbt_model.tests = []
    return dbt_model.tests


def init_metric(metrics: Dict[str, Metric], unique_id: str) -> Metric:
    if unique_id not in metrics:
        metrics[unique_id] = Metric(
            logical_id=MetricLogicalID(
                name=get_metric_name_from_unique_id(unique_id),
                type=MetricType.DBT_METRIC,
            )
        )
    return metrics[unique_id]


def init_schema(dataset: Dataset) -> None:
    if not dataset.schema:
        dataset.schema = DatasetSchema()
        dataset.schema.schema_type = SchemaType.SQL
        dataset.schema.fields = []


def init_field(fields: List[SchemaField], column: str) -> SchemaField:
    field = next((f for f in fields if f.field_path == column), None)
    if not field:
        field = SchemaField(field_path=column)
        fields.append(field)
    return field


def init_documentation(dataset: Dataset) -> None:
    if not dataset.documentation:
        dataset.documentation = DatasetDocumentation()
        dataset.documentation.dataset_documentations = []
        dataset.documentation.field_documentations = []


def init_field_doc(dataset: Dataset, column: str) -> FieldDocumentation:
    assert (
        dataset.documentation is not None
        and dataset.documentation.field_documentations is not None
    )

    doc = next(
        (
            d
            for d in dataset.documentation.field_documentations
            if d.field_path == column
        ),
        None,
    )
    if not doc:
        doc = FieldDocumentation()
        doc.field_path = column
        dataset.documentation.field_documentations.append(doc)
    return doc


def build_docs_url(docs_base_url: Optional[str], unique_id: str) -> Optional[str]:
    return f"{docs_base_url}/#!/model/{unique_id}" if docs_base_url else None


def build_source_code_url(
    project_source_url: Optional[str], file_path: str
) -> Optional[str]:
    return f"{project_source_url}/{file_path}" if project_source_url else None
