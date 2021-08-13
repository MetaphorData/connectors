import tempfile

from metaphor.models.metadata_change_event import (
    MetadataChangeEvent,
    Person,
    PersonLogicalID,
    PersonProperties,
)

from metaphor.common.file_sink import FileSink, FileSinkConfig
from tests.test_utils import load_json


def test_file_sink(test_root_dir):
    output = tempfile.mktemp(suffix=".json")

    message = MetadataChangeEvent(
        person=Person(
            logical_id=PersonLogicalID("foo@bar.com"),
            properties=PersonProperties(entity_id="abc", first_name="foo", last_name="bar", latest=False)
        ),
    )

    sink = FileSink(FileSinkConfig(output))
    assert sink.sink([message]) is True
    assert message == MetadataChangeEvent.from_dict(load_json(output)[0])
