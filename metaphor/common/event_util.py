import json
import logging
from datetime import datetime, timezone
from importlib import resources
from typing import Union

import fastjsonschema
from metaphor.models.metadata_change_event import (
    Dashboard,
    Dataset,
    EventHeader,
    KnowledgeCard,
    MetadataChangeEvent,
    Metric,
    Person,
    VirtualView,
)

from metaphor import models

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

ENTITY_TYPES = Union[Dashboard, Dataset, Metric, Person, KnowledgeCard, VirtualView]


class EventUtil:
    """Event utilities"""

    with resources.open_text(models, "metadata_change_event.json") as f:
        validate = fastjsonschema.compile(json.load(f))

    def __init__(self, extractor_class="", server=""):
        self._extractor_class = extractor_class
        self._server = server

    def _build_event(self, **kwargs) -> MetadataChangeEvent:
        """Create an MCE"""
        return MetadataChangeEvent(
            event_header=EventHeader(
                time=datetime.now(timezone.utc),
                app_name=self._extractor_class,
                server=self._server,
            ),
            **kwargs,
        )

    def build_event(self, entity: ENTITY_TYPES):
        """Build MCE given an entity"""
        if type(entity) is Dashboard:
            return self._build_event(dashboard=entity)
        elif type(entity) is Dataset:
            return self._build_event(dataset=entity)
        elif type(entity) is Person:
            return self._build_event(person=entity)
        elif type(entity) is KnowledgeCard:
            return self._build_event(knowledge_card=entity)
        elif type(entity) is VirtualView:
            return self._build_event(virtual_view=entity)
        else:
            raise TypeError(f"invalid entity type {type(entity)}")

    @staticmethod
    def validate_message(message: dict) -> bool:
        """Validate message against json schema"""
        try:
            EventUtil.validate(message)
        except fastjsonschema.JsonSchemaException as e:
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
