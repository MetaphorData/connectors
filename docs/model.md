# Data Model

Each connector ultimately outputs a JSON file that contains an array of so-called `MetadataChangeEvent`:

```text
[ event1, event2, ... ]
```

The file can be written to a local path during development or to an S3 bucket in production.

The remaining of this doc describes the event's structure and other details. However, you'll generally leverage the corresponding [Python Classes & Utility Methods](#python-classes--utility-methods) instead of handcrafting the JSON.

## MetadataChangeEvent

Think of `MetadataChangeEvent` as a nested object that describes the metadata associated with one or multiple entities, such as a dataset, dashboard, dbt model, etc. The overall structure looks like this:

```text
{
  eventHeader: ...
  dataset: ...
  dashboard: ...
  virtualView: ...
  ...
}
```

Other than `eventHeader`, all entity-specific fields (e.g. `dataset`, `dashboard`, etc) are optional. In fact, it is common for each `MetadataChangeEvent` to contain metadata on a single type of entity.

Each entity-specific field is an object that contains a required `logicalId`, and a list of optional "aspects." Using `dataset` as an example:

```text
{
  ...
  dataset: {
    logicalId: ...
    properties: ...
    schema: ...
    statistics: ...
    upstream: ...
    ...
  }
  ...
}
```

You use `logicalId` to specify which dataset the metadata aspects are associated with, and the aspects, in this case, can be a combination of `properties`, `schema`, `statistics` etc. It is also quite common to have only one of the aspects specified.

## Entity ID

The "logical ID" mentioned in the previous section is a JSON object containing multiple fields. For example, the `DatasetLogicalID` has the following structure:

```text
{
  name: "<dataset name>",
  platform: "<dataset platlform>",
  account: "<optional snowflake account>",
}
```

However, when referencing another logical ID in a metadata aspect, it's more common to use a string-based ID known as "Entity ID." This ID is used internally as the database's primary & foreign keys. An Entity ID can be directly generated from the corresponding logical ID by taking the MD5 hash it canonical JSON string (see [RFC 8785](https://datatracker.ietf.org/doc/html/rfc8785) for more details). The actual implementation is hidden from the users through utility methods such as `to_dataset_entity_id`.

## Python Classes & Utility Methods

We generate Python [data classes](https://docs.python.org/3/library/dataclasses.html) for `MetadataChangeEvent` and all its subfields and logical IDs. Using these classes will ensure that the generated JSON file will conform to the expected schema. All the fields also include [type hints](https://www.python.org/dev/peps/pep-0484/) to prevent incorrect assignments.

There are also several utilitiy methods defined in [event_utils.py](../metaphor/common/event_util.py) and [entity_id.py](../metaphor/common/entity_id.py) to simplify the building of `MetadataChangeEvent` and string-based entity IDs. 

## Example Events

### Lineage

Build an event that links `db.schema.src1` & `db.schema.src2` as the upstream lineage (sources) of `db.schema.dest` in Snowflake. 

```py
from metaphor.common.entity_id import to_dataset_entity_id
from metaphor.common.event_util import EventUtil
from metaphor.models.metadata_change_event import (
    Dataset,
    DatasetLogicalID,
    DataPlatform,
    DatasetUpstream,
)

# Create string-based entity IDs for src1 & src2
src1_id = str(to_dataset_entity_id('db.schema.src1', DataPlatform.SNOWFLAKE))
src2_id = str(to_dataset_entity_id('db.schema.src2', DataPlatform.SNOWFLAKE))

# Set the upstream aspect
dataset = Dataset(
    logical_id = DatasetLogicalID(name='db.schema.dest', platform=DataPlatform.SNOWFLAKE),
    upstream = DatasetUpstream(source_datasets=[src1_id, src2_id]),
)

# Build the final list of MetadataChangeEvents
event_util = EventUtil()
events = [event_util.build_event(dataset)]
```
