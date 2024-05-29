import tempfile
import traceback
from datetime import datetime
from typing import Callable, List, Optional, Tuple

from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.event_util import EventUtil
from metaphor.common.file_sink import FileSink, FileSinkConfig, S3StorageConfig
from metaphor.common.logger import get_logger
from metaphor.models.crawler_run_metadata import CrawlerRunMetadata, Platform, RunStatus
from metaphor.models.metadata_change_event import MetadataChangeEvent

logger = get_logger()


def run_connector(
    make_connector: Callable[[], BaseExtractor],
    name: str,
    description: str,
    platform: Optional[Platform] = None,
    file_sink_config: Optional[FileSinkConfig] = None,
) -> Tuple[List[MetadataChangeEvent], CrawlerRunMetadata]:
    """Run a connector and write the resulting events to files and/or API.

    Parameters
    ----------
    make_connector : Callable[[], BaseExtractor]
        The function to create and return an instance of connector
    name : str
        Name of the connector
    description : str
        Textual description of the connector
    platform : Optional[Platform]
        Platform of the connector
    file_sink_config : Optional[FileSinkConfig]
        Optional configuration for outputting events to files or cloud storage

    Returns
    -------
    list
        a list of MetadataChangeEvent written to the file sink
    """
    start_time = datetime.now()
    logger.info(f"Starting running {name} at {start_time}")

    # Write to a temp directory if not specified
    if file_sink_config is None:
        file_sink_config = FileSinkConfig(tempfile.mkdtemp())

    file_sink = FileSink(file_sink_config)
    query_log_sink = file_sink.get_query_log_sink()

    run_status = RunStatus.SUCCESS
    error_message = None
    stacktrace = None

    events: List[MetadataChangeEvent] = []
    connector: Optional[BaseExtractor] = None
    try:
        connector = make_connector()
        entities = connector.run_async()
        events = [EventUtil.build_event(entity) for entity in entities]
        file_sink.write_events(events)

        with query_log_sink:
            for query_log in connector.collect_query_logs():
                query_log_sink.write_query_log(query_log)

        if connector.status is RunStatus.FAILURE:
            logger.warning(f"Some of {name}'s entities cannot be parsed!")
            run_status = connector.status
            error_message = connector.error_message
            stacktrace = connector.stacktrace
    except Exception as ex:
        run_status = RunStatus.FAILURE
        error_message = str(ex)
        stacktrace = traceback.format_exc()
        logger.exception(ex)

    entity_count = len(events) + query_log_sink.total_mces_wrote
    end_time = datetime.now()
    logger.info(
        f"Ended running with {run_status} at {end_time}, "
        f"fetched {entity_count} entities, "
        f"took {format((end_time - start_time).total_seconds(), '.1f')}s"
    )

    run_metadata = CrawlerRunMetadata(
        crawler_name=name,
        platform=platform,
        description=description,
        start_time=start_time,
        end_time=end_time,
        status=run_status,
        entity_count=float(entity_count),
        error_message=error_message,
        stack_trace=stacktrace,
    )

    file_sink.write_metadata(run_metadata)
    file_sink.write_execution_logs()

    return events, run_metadata


def metaphor_file_sink_config(
    tenant: str,
    connector_name: str,
    is_metaphor_cloud=False,
    s3_auth_config=S3StorageConfig(),
) -> FileSinkConfig:
    """Create a FileSinkConfig for outputting events to a Metaphor tenant's cloud storage

    Parameters
    ----------
    tenant : str
        Name of the tenant
    connector_name : str
        Name of the connector
    is_metaphor_cloud : bool
        Whether this is a Metaphor cloud tenant (default is False)

    Returns
    -------
    FileSinkConfig
        the config created
    """

    bucket = (
        f"metaphor-mce-cloud-{tenant}"
        if is_metaphor_cloud
        else f"metaphor-mce-{tenant}"
    )
    return FileSinkConfig(
        directory=f"s3://{bucket}/{connector_name}", s3_auth_config=s3_auth_config
    )


def local_file_sink_config(directory: str) -> FileSinkConfig:
    """Create a FileSinkConfig for outputting events to a local directory

    Parameters
    ----------
    directory : str
        Path to a local directory

    Returns
    -------
    FileSinkConfig
        the config created
    """

    return FileSinkConfig(directory=directory)
