import traceback
from datetime import datetime
from typing import Collection, List, Optional, Tuple

from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.event_util import ENTITY_TYPES, EventUtil
from metaphor.common.logger import get_logger
from metaphor.common.sink import SinkConfig, StreamSink
from metaphor.common.storage import S3StorageConfig
from metaphor.models.crawler_run_metadata import CrawlerRunMetadata, Platform, RunStatus
from metaphor.models.metadata_change_event import MetadataChangeEvent

logger = get_logger()


def run_connector(
    connector: BaseExtractor,
    name: str,
    description: str,
    platform: Optional[Platform] = None,
    sink_config: Optional[SinkConfig] = None,
) -> Tuple[List[MetadataChangeEvent], CrawlerRunMetadata]:
    """Run a connector and write the resulting events to files and/or API.

    Parameters
    ----------
    connector: BaseExtractor
        The connector instance to run
    name : str
        Name of the connector
    description : str
        Textual description of the connector
    platform : Optional[Platform]
        Platform of the connector
    sink_config : Optional[SinkConfig]
        Optional configuration for outputting events to files or cloud storage

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
        entities = connector.run_async()
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

    if sink_config is not None:
        with StreamSink(sink_config, run_metadata) as sink:
            for event in events:
                sink.write_event(event)

    return events, run_metadata


def metaphor_sink_config(
    tenant: str,
    connector_name: str,
    is_metaphor_cloud=False,
    s3_auth_config=S3StorageConfig(),
) -> SinkConfig:
    """Create a SinkConfig for outputting events to a Metaphor tenant's cloud storage

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
    SinkConfig
        the config created
    """

    bucket = (
        f"metaphor-mce-cloud-{tenant}"
        if is_metaphor_cloud
        else f"metaphor-mce-{tenant}"
    )
    return SinkConfig(
        directory=f"s3://{bucket}/{connector_name}", s3_auth_config=s3_auth_config
    )


def local_sink_config(directory: str) -> SinkConfig:
    """Create a SinkConfig for outputting events to a local directory

    Parameters
    ----------
    directory : str
        Path to a local directory

    Returns
    -------
    SinkConfig
        the config created
    """

    return SinkConfig(directory=directory)
