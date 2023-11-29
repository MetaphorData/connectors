from typing import Dict
from metaphor.models.metadata_change_event import SchemaField

from metaphor.common.logger import get_logger

logger = get_logger()

class ProtobufParser:

    @staticmethod
    def parse(raw_schema: str, subject: str):
        pass
        # TODO see how open metadata does it:
        # https://github.com/open-metadata/OpenMetadata/blob/27fa5b34bb873d3e9b309c7fd80363827b798ab8/ingestion/src/metadata/parsers/protobuf_parser.py#L88
