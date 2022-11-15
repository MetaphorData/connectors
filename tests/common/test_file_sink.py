import tempfile
from datetime import datetime
from os import path
from zipfile import ZipFile

from metaphor.common.event_util import EventUtil
from metaphor.common.file_sink import FileSink, FileSinkConfig
from metaphor.common.logger import add_debug_file
from metaphor.models.crawler_run_metadata import CrawlerRunMetadata, Status
from metaphor.models.metadata_change_event import (
    MetadataChangeEvent,
    Person,
    PersonLogicalID,
)
from tests.test_utils import load_json


def events_from_json(file):
    return [MetadataChangeEvent.from_dict(json) for json in load_json(file)]


def test_file_sink_no_split(test_root_dir):
    directory = tempfile.mkdtemp()

    messages = [
        MetadataChangeEvent(person=Person(logical_id=PersonLogicalID("foo1@bar.com"))),
        MetadataChangeEvent(person=Person(logical_id=PersonLogicalID("foo2@bar.com"))),
    ]

    sink = FileSink(FileSinkConfig(directory=directory, batch_size=2))
    assert sink.sink(messages) is True
    assert messages == events_from_json(f"{directory}/1-of-1.json")


def test_file_sink_split(test_root_dir):
    directory = tempfile.mkdtemp()

    messages = [
        MetadataChangeEvent(person=Person(logical_id=PersonLogicalID("foo1@bar.com"))),
        MetadataChangeEvent(person=Person(logical_id=PersonLogicalID("foo2@bar.com"))),
        MetadataChangeEvent(person=Person(logical_id=PersonLogicalID("foo3@bar.com"))),
        MetadataChangeEvent(person=Person(logical_id=PersonLogicalID("foo4@bar.com"))),
        MetadataChangeEvent(person=Person(logical_id=PersonLogicalID("foo5@bar.com"))),
    ]

    sink = FileSink(FileSinkConfig(directory=directory, batch_size=2))
    assert sink.sink(messages) is True
    assert messages[0:2] == events_from_json(f"{directory}/1-of-3.json")
    assert messages[2:4] == events_from_json(f"{directory}/2-of-3.json")
    assert messages[4:] == events_from_json(f"{directory}/3-of-3.json")


def test_sink_metadata(test_root_dir):
    directory = tempfile.mkdtemp()

    metadata = CrawlerRunMetadata(
        crawler_name="foo",
        description="bar",
        start_time=datetime.now(),
        end_time=datetime.now(),
        status=Status.SUCCESS,
        entity_count=1.0,
    )

    sink = FileSink(FileSinkConfig(directory=directory, batch_size=2))
    sink.sink_metadata(metadata)

    assert EventUtil.clean_nones(metadata.to_dict()) == load_json(
        f"{directory}/run.metadata"
    )


def test_sink_logs(test_root_dir):
    _, debug_file = tempfile.mkstemp()
    add_debug_file(debug_file)

    directory = tempfile.mkdtemp()

    sink = FileSink(FileSinkConfig(directory=directory, batch_size=2))
    sink.sink_logs()

    zip_file = f"{directory}/log.zip"

    assert path.exists(zip_file)
    with ZipFile(zip_file) as file:
        base_names = set([path.basename(name) for name in file.namelist()])

    assert path.basename(f"{path.basename(directory)}.log") in base_names
    assert path.basename(debug_file) in base_names


def test_sink_file(test_root_dir):
    directory = tempfile.mkdtemp()

    sink = FileSink(FileSinkConfig(directory=directory, batch_size=2))
    filename = "test.txt"
    sink.write_file(filename, "the content")

    full_path = f"{directory}/{filename}"
    assert path.exists(full_path)

    with open(full_path) as f:
        content = f.read()
    assert content == "the content"

    sink.remove_file(filename)
    assert path.exists(full_path) is False
