# Adding a Custom Connector

The `metaphor-connectors` package comes with a list of connectors for many of the commonly used data systems. You can also use the package to build your own custom connector to fetch metadata from other systems.

## Connector Function

Each custom connector should expose a function that takes no input argument and returns a collection of supported "entities" (e.g. datasets, dashboards, dbt models, etc.). For example,

```py
def custom_connector() -> Collection[ENTITY_TYPES]:
    # fetch metadata from the system and build a list of entities
    datasets = []
    for item in fetch_metadata():
        datasets.append(map_item_to_dataset(item))
  
    return datasets
```

## Entity Classes

We generate Python [data classes](https://docs.python.org/3/library/dataclasses.html) with [type hints](https://www.python.org/dev/peps/pep-0484/), so you can simply use the entity's constructor to build the object. For example,

```py
from metaphor.models.metadata_change_event import (
    Dataset,
    EntityUpstream,
    ...
)


dataset = Dataset(
    logical_id=...,
    schema=...,
    statistics=...,
    entity_upstream=...,
)
```

 are associated with, and the aspects, in this case, can be a combination of `properties`, `schema`, `statistics` etc. It is also quite common to have only one of the aspects specified.

You use `logicalId` (more in next section) to specify which entity the metadata "aspects" (`schema`, `statistics`, `statistics` etc.) are associated with. As all aspects are optional, you can pick and choose which ones your customer connector would like to publish. Note that if multiple connectors try to publish to the same aspect for the same entity, the last one published will "win", i.e. no automatic merging or conflict resolution will happen.

## Entity & Logical IDs

The `logicalId` mentioned in the previous section is an object containing multiple fields. For example, the `DatasetLogicalID` has the following structure when converted to JSON:

```json
{
  "name": "<dataset name>",
  "platform": "<dataset platform>",
  "account": "<optional snowflake account>",
}
```

However, when referencing another logical ID inside a metadata aspect, it's more common to use a string-based ID known as "Entity ID." This ID is used internally as the database's primary & foreign keys. An Entity ID can be directly generated from the corresponding logical ID by taking the MD5 hash of its canonical JSON string (see [RFC 8785](https://datatracker.ietf.org/doc/html/rfc8785) for more details). The actual implementation is hidden from the users through utility methods such as [`to_dataset_entity_id`](../metaphor/common/entity_id.py), e.g.

```py
from metaphor.common.entity_id import dataset_normalized_name, to_dataset_entity_id
from metaphor.models.metadata_change_event import DataPlatform


entityId = to_dataset_entity_id(
    name=dataset_normalized_name("db1", "schema1", "table1"),
    platform=DataPlatform.SNOWFLAKE,
    account="my_snowflake_account",
)
```

## Metadata Change Event (MCE)

When all the entities are finally published, they are converted to a JSON array of `MetadataChangeEvent`s. Each `MetadataChangeEvent` is a JSON object that contains up to one entity for each supported entity type. The overall structure looks like this:

```json
[
  {
    "dataset": {...},
    "dashboard": {...},
    "virtualView": {...}
  },
  {
    "dataset": {...},
    "dashboard": {...},
    "virtualView": {...}
  }
]
```

All entity-specific fields (e.g. `dataset`, `dashboard`, etc) are optional. In fact, it is common for each `MetadataChangeEvent` to contain metadata only for a single type of entity.

## Outputting Events

To output the entities to a local path (for development) or an S3 bucket (for production) you can use the [`run_connector`](../metaphor/common/runner.py) wrapper function to execute your connector function:

```py
from metaphor.common.runner import run_connector
from metaphor.models.crawler_run_metadata import Platform


run_connector(
    connector_func=custom_connector,  # custom_connector from above example
    name="connector_name",  # name should contain only alphanumeric characters plus underscores
    platform=Platform.OTHER,  # use other unless there's a matching platform
    description="connector description",  # user-facing description for the connector
    file_sink_config=local_file_sink_config("/path/to/output"),  
)
```

Once the output content has been verified, you can change [`local_file_sink_config`](../metaphor/common/runner.py) to [`metaphor_file_sink_config`](../metaphor/common/runner.py) to publish to S3 buckets.

## Logging

We provide a custom logger such that all logs will be automatically formatted and outputted as a zip file along with the events. Use the following code to leverage the logger:

```py
from metaphor.common.logger import get_logger


logger = get_logger()
logger.info('info message')
logger.warning('warning message')
logger.error('error message')
```

## Examples

### Custom Lineage Connector

Here is a [full example](../examples/custom_lineage.py) that showcases a custom connector that publishes the lineage. It specifies `db.schema.src1` & `db.schema.src2` as the upstream lineage (sources) of `db.schema.dest` in BigQuery.

### Custom Data Quality Connector

Here is [another example](../examples/custom_dq.py) demonstrating a custom connector that publishes data quality metadata.

### Custom Dashboard Connector

[This example](../examples/custom_dashboard.py) shows how to publish metadata for a custom dashboard.
