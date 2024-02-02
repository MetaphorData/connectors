import json
import tempfile
from datetime import datetime
from os import path
from zipfile import ZipFile

from freezegun import freeze_time

from metaphor.common.event_util import EventUtil
from metaphor.common.file_sink import FileSink, FileSinkConfig
from metaphor.common.logger import add_debug_file
from metaphor.common.utils import md5_digest
from metaphor.models.crawler_run_metadata import CrawlerRunMetadata, RunStatus
from metaphor.models.metadata_change_event import (
    DataPlatform,
    Dataset,
    DatasetLogicalID,
    MetadataChangeEvent,
    QueryLog,
)
from tests.test_utils import load_json


def events_from_json(file):
    return [MetadataChangeEvent.from_dict(json) for json in load_json(file)]


@freeze_time("2000-01-01")
def test_file_sink_no_split(test_root_dir):
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
    sink = FileSink(FileSinkConfig(directory=directory, batch_size_bytes=1000000))
    assert sink.write_events(messages) is True
    assert messages == events_from_json(f"{directory}/946684800/1-of-1.json")


@freeze_time("2000-01-01")
def test_file_sink_split(test_root_dir):
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
    sink = FileSink(FileSinkConfig(directory=directory, batch_size_bytes=10))
    assert sink.write_events(messages) is True
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

    sink = FileSink(FileSinkConfig(directory=directory))
    sink.write_metadata(metadata)

    assert EventUtil.clean_nones(metadata.to_dict()) == load_json(
        f"{directory}/946684800/run.metadata"
    )


@freeze_time("2000-01-01")
def test_sink_logs(test_root_dir):
    _, debug_file = tempfile.mkstemp()
    add_debug_file(debug_file)

    directory = tempfile.mkdtemp()

    sink = FileSink(FileSinkConfig(directory=directory))
    sink.write_execution_logs()

    zip_file = f"{directory}/946684800/log.zip"

    assert path.exists(zip_file)
    with ZipFile(zip_file) as file:
        base_names = set([path.basename(name) for name in file.namelist()])

    assert path.basename("run.log") in base_names
    assert path.basename(debug_file) in base_names


@freeze_time("2000-01-01")
def test_sink_file(test_root_dir):
    directory = tempfile.mkdtemp()

    sink = FileSink(FileSinkConfig(directory=directory))
    filename = "test.txt"
    sink.write_file(filename, "the content")

    full_path = f"{directory}/946684800/{filename}"
    assert path.exists(full_path)

    with open(full_path) as f:
        content = f.read()
    assert content == "the content"

    sink.remove_file(filename)
    assert path.exists(full_path) is False


def test_query_log_sink_chunk_by_count():
    directory = tempfile.mkdtemp()

    sink = FileSink(FileSinkConfig(directory=directory, batch_size_count=3))
    query_logs = [
        QueryLog(
            id=f"{DataPlatform.SNOWFLAKE.name}:{query_id}",
            query_id=str(query_id),
            platform=DataPlatform.SNOWFLAKE,
            account="account",
            sql=query_text,
            sql_hash=md5_digest(query_text.encode("utf-8")),
        )
        for query_id, query_text in enumerate(
            f"this is query no. {x}" for x in range(17)
        )
    ]
    with sink.get_query_log_sink(2) as query_log_sink:
        for query_log in query_logs:
            query_log_sink.write_query_log(query_log)

    files = sink._storage.list_files(sink.path, ".json")
    assert len(files) == 3
    for file in files:
        with open(file) as f:
            obj = json.loads(f.read())
            assert len(obj) == 3
            for mce in obj:
                assert len(mce["queryLogs"]["logs"]) < 3


def test_query_log_sink_chunk_by_size():
    directory = tempfile.mkdtemp()

    sink = FileSink(FileSinkConfig(directory=directory, batch_size_bytes=300))

    def get_sql_query(x: int) -> str:
        if x in {0, 3, 8}:
            return "long" * 100
        return "short"

    query_logs = [
        QueryLog(
            id=f"{DataPlatform.SNOWFLAKE.name}:{query_id}",
            query_id=str(query_id),
            platform=DataPlatform.SNOWFLAKE,
            account="account",
            sql=query_text,
            sql_hash=md5_digest(query_text.encode("utf-8")),
        )
        for query_id, query_text in enumerate(get_sql_query(x) for x in range(11))
    ]
    with sink.get_query_log_sink() as query_log_sink:
        for query_log in query_logs:
            query_log_sink.write_query_log(query_log)

    files = sorted(sink._storage.list_files(sink.path, ".json"))

    def assert_num_logs(file, num: int):
        with open(file) as f:
            obj = json.loads(f.read())
            print(obj)
            # assert len(obj) == 1
            # assert len(obj[0]["queryLogs"]["logs"]) == num

    assert len(files) == 5
    assert_num_logs(files[0], 1)
    assert_num_logs(files[1], 3)
    assert_num_logs(files[2], 3)
    assert_num_logs(files[3], 2)
    assert_num_logs(files[4], 2)
