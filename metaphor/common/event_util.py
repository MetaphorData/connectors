import json
import logging
from datetime import datetime, timezone
from importlib import resources

import fastjsonschema
from metaphor.models.metadata_change_event import (
    Dashboard,
    Dataset,
    EventHeader,
    KnowledgeCard,
    MetadataChangeEvent,
    Person,
)

from metaphor import models

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class EventUtil:
    """Event utilities"""

    with resources.open_text(models, "metadata_change_event.json") as f:
        validate = fastjsonschema.compile(json.load(f))

    @staticmethod
    def _build_event(**kwargs) -> MetadataChangeEvent:
        """Create an MCE"""
        return MetadataChangeEvent(
            event_header=EventHeader(time=datetime.now(timezone.utc)), **kwargs
        )

    @staticmethod
    def build_dashboard_event(entity: Dashboard) -> MetadataChangeEvent:
        """Build MCE given a dashboard"""
        return EventUtil._build_event(dashboard=entity)

    @staticmethod
    def build_dataset_event(entity: Dataset) -> MetadataChangeEvent:
        """Build MCE given a dataset"""
        return EventUtil._build_event(dataset=entity)

    @staticmethod
    def build_person_event(entity: Person) -> MetadataChangeEvent:
        """Build MCE given a person"""
        return EventUtil._build_event(person=entity)

    @staticmethod
    def build_knowledge_card_event(entity: KnowledgeCard) -> MetadataChangeEvent:
        """Build MCE given a knowledge card"""
        return EventUtil._build_event(knowledge_card=entity)

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
    def trim_event(event: MetadataChangeEvent) -> dict:
        """Cast event to dict and remove all None values"""
        return EventUtil.clean_nones(event.to_dict())

    @staticmethod
    def serialize_event(event: MetadataChangeEvent) -> str:
        """Serialize event to json string"""
        return json.dumps(EventUtil.trim_event(event))
