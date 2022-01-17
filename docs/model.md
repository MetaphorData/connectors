# Data Model

Each connector ultimiately outpus a JSON file that contains an array of so-called `MetadataChangeEvent`:

```text
[ event1, event2, ... ]
```

The file can be written to a local path during development or to a S3 bucket in production.

The reamining of this doc describes the structure of the event and other detials. However, in general, you'll leverage the corresponding [Python Classes & Utility Methods](#python-classes--utility-methods) instead of handcrafting the JSON.

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

Other than `eventHeader`, all other entity-specific fields (e.g. `dataset`, `dashboard`, etc) are optional. In fact, it is common for each `MetadataChangeEvent` to contain metadata on a single type of entity.

Each entity-specific field is an object that copntains a required `logicalId`, and a list of optional "aspects". Using `dataset` as an example:

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

You use `logicalId` to specify which dataset the metadata aspects are associated to, and the aspects in this case can be a combination of `properties`, `schema`, `statistics` etc. It is also quite common to have only one of the aspects specified.

## Entity ID



## Python Classes & Utility Methods

We generate Python [data classes](https://docs.python.org/3/library/dataclasses.html) for `MetadataChangeEvent` and all its subfields. Using these classes will ensure that the generated JSON file will conform to the expected schema. All the fields also include [type hints](https://www.python.org/dev/peps/pep-0484/) to prevent incorrect assingments.

There are also several utilitiy methods defined in [event_utils.py](../blob/main/metaphor/common/event_util.py) and [entity_id.py](../blob/main/metaphor/common/entity_id.py) to simplify the building of `MetadataChangeEvent` and string-based entity IDs. 

## Example Events

### Lineage

Build an event that links `db.schema.src1` & `db.schema.src2` as the upstream lineage (sources) of `db.schema.dest` in Snowflake. 

```py
from metaphor.common.entity_id import to_dataset_entity_id
from metaphor.common.event_util import EventUtil
from metaphor.models.metadata_change_event import (
    Dataset,
    DatasetLogicalID,
    DatasetPlatform,
    DatasetUpstream,
    MetadataChangeEvent,
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
events = [EventUtil.build_dataset_event(dataset)]
```
