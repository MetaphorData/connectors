import json
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable, Dict, List, Optional

from metaphor.common.entity_id import (
    EntityId,
    dataset_normalized_name,
    to_dataset_entity_id,
    to_dataset_entity_id_from_logical_id,
    to_person_entity_id,
    to_virtual_view_entity_id,
)
from metaphor.common.logger import get_logger
from metaphor.common.utils import is_email
from metaphor.dbt.config import MetaOwnership, MetaTag
from metaphor.models.metadata_change_event import (
    DataMonitor,
    DataMonitorStatus,
    DataMonitorTarget,
    DataPlatform,
    DataQualityProvider,
    Dataset,
    DatasetDataQuality,
    DatasetDocumentation,
    DatasetLogicalID,
    DatasetSchema,
    DbtTest,
    FieldDocumentation,
    Metric,
    MetricLogicalID,
    MetricType,
    Ownership,
    SchemaField,
    SchemaType,
    SystemTag,
    SystemTags,
    SystemTagSource,
    VirtualView,
    VirtualViewLogicalID,
    VirtualViewType,
)

logger = get_logger()


def get_dataset_entity_id(self, db: str, schema: str, table: str) -> EntityId:
    return to_dataset_entity_id(
        dataset_normalized_name(db, schema, table), self._platform, self._account
    )


def get_virtual_view_id(logical_id: VirtualViewLogicalID) -> str:
    assert logical_id.name and logical_id.type
    return str(to_virtual_view_entity_id(logical_id.name, logical_id.type))


def get_model_name_from_unique_id(unique_id: str) -> str:
    assert unique_id.startswith("model."), f"invalid model id {unique_id}"
    return unique_id[6:]


def get_metric_name_from_unique_id(unique_id: str) -> str:
    assert unique_id.startswith("metric."), f"invalid metric id {unique_id}"
    return unique_id[7:]


def get_snapshot_name_from_unique_id(unique_id: str) -> str:
    assert unique_id.startswith("snapshot."), f"invalid snapshot id {unique_id}"
    return unique_id[9:]


@dataclass
class Ownerships:
    dbt_model: List[Ownership] = field(default_factory=lambda: [])
    materialized_table: List[Ownership] = field(default_factory=lambda: [])


def get_ownerships_from_meta(
    meta: Dict, meta_ownerships: List[MetaOwnership]
) -> Ownerships:
    """Extracts ownership info from models' meta field"""

    def to_owner(email_or_username: str, email_domain: Optional[str]) -> Optional[str]:
        email = email_or_username
        if "@" not in email_or_username and email_domain is not None:
            email = f"{email_or_username}@{email_domain}"

        if not is_email(email):
            logger.warning(f"Skipping invalid email address: {email}")
            return None

        return str(to_person_entity_id(email))

    ownerships = Ownerships()
    for meta_ownership in meta_ownerships:
        value = meta.get(meta_ownership.meta_key)
        email_or_usernames = []
        if isinstance(value, str):
            email_or_usernames.append(value)
        elif isinstance(value, list):
            email_or_usernames.extend(value)

        for email_or_username in email_or_usernames:
            owner = to_owner(email_or_username, meta_ownership.email_domain)
            if owner is not None:
                ownership = Ownership(
                    contact_designation_name=meta_ownership.ownership_type,
                    person=owner,
                )
                if meta_ownership.assignment_target in ["dbt_model", "both"]:
                    ownerships.dbt_model.append(ownership)
                if meta_ownership.assignment_target in ["materialized_table", "both"]:
                    ownerships.materialized_table.append(ownership)

    return ownerships


def get_metaphor_tags_from_meta(meta: Dict, meta_tags: List[MetaTag]) -> List[str]:
    """Extracts metaphor tag info from models' meta field"""

    tags: List[str] = []
    for meta_tag in meta_tags:
        value = meta.get(meta_tag.meta_key)
        if re.fullmatch(meta_tag.meta_value_matcher, str(value)):
            tags.append(meta_tag.tag_type)

    return tags


def get_dbt_tags_from_meta(
    meta: Optional[Dict], meta_key_tags: Optional[str]
) -> List[str]:
    """Extracts dbt tags from models' meta field"""

    if meta is None or meta_key_tags is None:
        return []

    value = meta.get(meta_key_tags)
    if value is None:
        return []

    # Value is list of string
    if isinstance(value, list):
        return [str(item) for item in value]

    # Value is a single string
    if isinstance(value, str):
        return [value]

    raise ValueError(f"Unexpected type for meta.{meta_key_tags}: {type(value)}")


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
                name=dataset_normalized_name(database, schema, name),
                platform=platform,
                account=account,
            )
        )
    return datasets[unique_id]


def init_virtual_view(
    virtual_views: Dict[str, VirtualView],
    unique_id: str,
    id_to_name_func: Callable[[str], str],
) -> VirtualView:
    if unique_id not in virtual_views:
        virtual_views[unique_id] = VirtualView(
            logical_id=VirtualViewLogicalID(
                name=id_to_name_func(unique_id),
                type=VirtualViewType.DBT_MODEL,
            ),
        )
    return virtual_views[unique_id]


def init_dbt_tests(
    virtual_views: Dict[str, VirtualView], dbt_model_unique_id: str
) -> List[DbtTest]:
    assert dbt_model_unique_id in virtual_views

    dbt_model = virtual_views[dbt_model_unique_id].dbt_model
    if dbt_model is None:
        return []

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
        field = SchemaField(field_path=column, field_name=column, subfields=None)
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


def build_model_docs_url(docs_base_url: Optional[str], unique_id: str) -> Optional[str]:
    return f"{docs_base_url}/#!/model/{unique_id}" if docs_base_url else None


def build_metric_docs_url(
    docs_base_url: Optional[str], unique_id: str
) -> Optional[str]:
    return f"{docs_base_url}/#!/metric/{unique_id}" if docs_base_url else None


def build_source_code_url(
    project_source_url: Optional[str], file_path: str
) -> Optional[str]:
    return f"{project_source_url}/{file_path}" if project_source_url else None


def build_system_tags(tag_names: List[str]) -> Optional[SystemTags]:
    tags = [
        SystemTag(
            key=None,
            system_tag_source=SystemTagSource.DBT,
            value=name,
        )
        for name in tag_names
        if name
    ]
    return SystemTags(tags=tags)


def add_data_quality_monitor(
    dataset: Dataset,
    name: str,
    column_name: Optional[str],
    status: DataMonitorStatus,
    last_run: Optional[datetime],
) -> None:
    if dataset.data_quality is None:
        dataset.data_quality = DatasetDataQuality(
            monitors=[], provider=DataQualityProvider.DBT
        )

    assert dataset.data_quality.monitors is not None
    assert dataset.logical_id

    dataset.data_quality.monitors.append(
        # For `DataMonitorTarget`:
        # column: Name of the target column. Not set if the monitor performs dataset-level tests, e.g. row count.
        # dataset: Entity ID of the target dataset. Set only if the monitor uses a different dataset from the one the data quality metadata is attached to.
        DataMonitor(
            last_run=last_run,
            title=name,
            targets=[
                DataMonitorTarget(
                    dataset=str(
                        to_dataset_entity_id_from_logical_id(dataset.logical_id)
                    ),
                    column=column_name,
                )
            ],
            status=status,
        )
    )


def get_data_platform_from_manifest(
    manifest_file: str,
):
    with open(manifest_file) as f:
        manifest_json = json.load(f)
    manifest_metadata = manifest_json.get("metadata", {})
    platform = manifest_metadata.get("adapter_type", "").upper()
    assert platform in DataPlatform.__members__, f"Invalid data platform {platform}"
    return DataPlatform[platform]
