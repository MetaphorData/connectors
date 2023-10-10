import traceback
from datetime import datetime
from typing import Callable, Collection, List, Optional

from metaphor.common.api_sink import ApiSink, ApiSinkConfig
from metaphor.common.event_util import ENTITY_TYPES, EventUtil
from metaphor.common.file_sink import FileSink, FileSinkConfig, S3StorageConfig
from metaphor.common.logger import get_logger
from metaphor.models.crawler_run_metadata import CrawlerRunMetadata, Platform, RunStatus
from metaphor.models.metadata_change_event import MetadataChangeEvent

logger = get_logger()


def run_connector(
    connector_func: Callable[[], Collection[ENTITY_TYPES]],
    name: str,
    platform: Platform,
    description: str,
    file_sink_config: Optional[FileSinkConfig] = None,
    api_sink_config: Optional[ApiSinkConfig] = None,
) -> List[MetadataChangeEvent]:
    """Run a connector and write the resulting events to files and/or API.

    Parameters
    ----------
    connector_func : Callable[[], Collection[ENTITY_TYPES]]
        The connector function to run
    name : str
        Name of the connector
    platform : Platform
        Platform of the connector
    description : str
        Textual description of the connector
    file_sink_config : Optional[FileSinkConfig]
        Optional configuration for outputting events to files or cloud storage
    api_sink_config : Optional[ApiSinkConfig]
        (Deprecated) Optional  configuration for outputting events to an API

    Returns
    -------
    list
        a list of MetadataChangeEvent written to the file sink
    """
    start_time = datetime.now()
    logger.info(f"Starting running {name} at {start_time}")

    run_status = RunStatus.SUCCESS
    error_message = None
    stacktrace = None

    entities: Collection[ENTITY_TYPES] = []
    try:
        entities = connector_func()
    except Exception as ex:
        run_status = RunStatus.FAILURE
        error_message = str(ex)
        stacktrace = traceback.format_exc()
        logger.exception(ex)

    end_time = datetime.now()
    entity_count = len(entities)
    logger.info(
        f"Ended running with {run_status} at {end_time}, fetched {entity_count} entities, took {format((end_time - start_time).total_seconds(), '.1f')}s"
    )

    events = [EventUtil.build_event(entity) for entity in entities]

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

    if file_sink_config is not None:
        file_sink = FileSink(file_sink_config)
        file_sink.sink(events)
        file_sink.sink_metadata(run_metadata)
        file_sink.sink_logs()

    if api_sink_config is not None:
        api_sink = ApiSink(api_sink_config)
        api_sink.sink(events)

    return events


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
