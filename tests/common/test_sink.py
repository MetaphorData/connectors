import tempfile
from datetime import datetime
from os import path
from zipfile import ZipFile

import pytest
from freezegun import freeze_time

from metaphor.common.event_util import EventUtil
from metaphor.common.logger import add_debug_file
from metaphor.common.sink import SinkConfig, StreamSink
from metaphor.models.crawler_run_metadata import CrawlerRunMetadata, RunStatus
from metaphor.models.metadata_change_event import (
    DataPlatform,
    Dataset,
    DatasetLogicalID,
    MetadataChangeEvent,
)
from tests.test_utils import load_json


def events_from_json(file):
    return [MetadataChangeEvent.from_dict(json) for json in load_json(file)]


@freeze_time("2000-01-01")
def test_file_sink_no_split(test_root_dir: str):
    directory = tempfile.mkdtemp()

    messages = [
        MetadataChangeEvent(
            dataset=Dataset(
                logical_id=DatasetLogicalID(name="foo1", platform=DataPlatform.BIGQUERY)
            )
        ),
        MetadataChangeEvent(
            dataset=Dataset(
                logical_id=DatasetLogicalID(name="foo2", platform=DataPlatform.BIGQUERY)
            )
        ),
    ]

    # Set batch_size_bytes so large that all messages can fit in the same file
    with StreamSink(SinkConfig(directory=directory, batch_size_bytes=1000000)) as sink:
        for event in messages:
            assert sink.write_event(event)
    assert messages == events_from_json(f"{directory}/946684800/1-of-1.json")


@freeze_time("2000-01-01")
def test_file_sink_split(test_root_dir: str):
    directory = tempfile.mkdtemp()

    messages = [
        MetadataChangeEvent(
            dataset=Dataset(
                logical_id=DatasetLogicalID(name="foo1", platform=DataPlatform.BIGQUERY)
            )
        ),
        MetadataChangeEvent(
            dataset=Dataset(
                logical_id=DatasetLogicalID(name="foo2", platform=DataPlatform.BIGQUERY)
            )
        ),
        MetadataChangeEvent(
            dataset=Dataset(
                logical_id=DatasetLogicalID(name="foo3", platform=DataPlatform.BIGQUERY)
            )
        ),
        MetadataChangeEvent(
            dataset=Dataset(
                logical_id=DatasetLogicalID(name="foo4", platform=DataPlatform.BIGQUERY)
            )
        ),
        MetadataChangeEvent(
            dataset=Dataset(
                logical_id=DatasetLogicalID(name="foo5", platform=DataPlatform.BIGQUERY)
            )
        ),
    ]

    # Set batch_size_bytes so small that only one message can be fit in each file
    with StreamSink(SinkConfig(directory=directory, batch_size_bytes=10)) as sink:
        for message in messages:
            assert sink.write_event(message) is True
    assert messages[0:1] == events_from_json(f"{directory}/946684800/1-of-5.json")
    assert messages[1:2] == events_from_json(f"{directory}/946684800/2-of-5.json")
    assert messages[2:3] == events_from_json(f"{directory}/946684800/3-of-5.json")
    assert messages[3:4] == events_from_json(f"{directory}/946684800/4-of-5.json")
    assert messages[4:5] == events_from_json(f"{directory}/946684800/5-of-5.json")


@freeze_time("2000-01-01")
def test_sink_metadata(test_root_dir):
    directory = tempfile.mkdtemp()

    metadata = CrawlerRunMetadata(
        crawler_name="foo",
        description="bar",
        start_time=datetime.now(),
        end_time=datetime.now(),
        status=RunStatus.SUCCESS,
        entity_count=1.0,
    )

    with StreamSink(SinkConfig(directory=directory), metadata):
        # It's gonna write it when exiting the context
        pass

    assert EventUtil.clean_nones(metadata.to_dict()) == load_json(
        f"{directory}/946684800/run.metadata"
    )


@freeze_time("2000-01-01")
def test_sink_logs(test_root_dir):
    _, debug_file = tempfile.mkstemp()
    add_debug_file(debug_file)

    directory = tempfile.mkdtemp()

    with StreamSink(SinkConfig(directory=directory)):
        # It's gonna write it when exiting the context
        pass

    zip_file = f"{directory}/946684800/log.zip"

    assert path.exists(zip_file)
    with ZipFile(zip_file) as file:
        base_names = set([path.basename(name) for name in file.namelist()])

    assert path.basename("run.log") in base_names
    assert path.basename(debug_file) in base_names


def test_stream_sink_not_in_context_manager():
    directory = tempfile.mkdtemp()
    with pytest.raises(ValueError):
        sink = StreamSink(SinkConfig(directory=directory))
        sink.write_event(
            MetadataChangeEvent(
                dataset=Dataset(
                    logical_id=DatasetLogicalID(
                        name="foo1", platform=DataPlatform.BIGQUERY
                    )
                )
            )
        )
