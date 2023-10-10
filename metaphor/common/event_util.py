import json
import logging
from importlib import resources
from typing import Union

from jsonschema import ValidationError
from jsonschema.validators import validator_for

from metaphor import models  # type: ignore
from metaphor.models.metadata_change_event import (
    Dashboard,
    Dataset,
    Hierarchy,
    KnowledgeCard,
    MetadataChangeEvent,
    Metric,
    Pipeline,
    QueryLogs,
    VirtualView,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

ENTITY_TYPES = Union[
    Dashboard, Dataset, Metric, KnowledgeCard, VirtualView, QueryLogs, Hierarchy
]


class EventUtil:
    """Event utilities"""

    def __init__(self):
        with resources.open_text(models, "metadata_change_event.json") as f:
            mce_schema = json.load(f)

        validator_class = validator_for(mce_schema)
        validator_class.check_schema(mce_schema)
        self._validator = validator_class(mce_schema)

    @staticmethod
    def _build_event(**kwargs) -> MetadataChangeEvent:
        """Create an MCE"""
        return MetadataChangeEvent(**kwargs)

    @staticmethod
    def build_event(entity: ENTITY_TYPES):
        """Build MCE given an entity"""
        if type(entity) is Dashboard:
            return EventUtil._build_event(dashboard=entity)
        elif type(entity) is Dataset:
            return EventUtil._build_event(dataset=entity)
        elif type(entity) is Metric:
            return EventUtil._build_event(metric=entity)
        elif type(entity) is Pipeline:
            return EventUtil._build_event(pipeline=entity)
        elif type(entity) is KnowledgeCard:
            return EventUtil._build_event(knowledge_card=entity)
        elif type(entity) is VirtualView:
            return EventUtil._build_event(virtual_view=entity)
        elif type(entity) is QueryLogs:
            return EventUtil._build_event(query_logs=entity)
        elif type(entity) is Hierarchy:
            return EventUtil._build_event(hierarchy=entity)
        else:
            raise TypeError(f"invalid entity type {type(entity)}")

    def validate_message(self, message: dict) -> bool:
        """Validate message against json schema"""
        try:
            self._validator.validate(message)
        except ValidationError as e:
            logger.error(f"MCE validation error: {e}. Message: {message}")
            return False
        return True

    @staticmethod
    def clean_nones(value):
        """
        Recursively remove all None values from dictionaries and lists, and returns
        the result as a new dictionary or list.
        """
        if isinstance(value, list):
            return [EventUtil.clean_nones(x) for x in value if x is not None]
        elif isinstance(value, dict):
            return {
                key: EventUtil.clean_nones(val)
                for key, val in value.items()
                if val is not None
            }
        else:
            return value

    @staticmethod
    def trim_event(event: Union[MetadataChangeEvent, ENTITY_TYPES]) -> dict:
        """Cast event to dict and remove all None values"""
        return EventUtil.clean_nones(event.to_dict())

    @staticmethod
    def class_fqcn(clazz) -> str:
        """Get the fully qualified class name"""
        return f"{clazz.__module__}.{clazz.__qualname__}"
