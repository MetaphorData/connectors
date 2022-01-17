# Data Model

Each connector ultimiately outpus a JSON file that contains an array of so-called `MetadataChangeEvent`:

```text
[ event1, event2, ... ]
```

The file can be written to a local path during development or to a S3 bucket in production.

The reamining of this doc describes the structure of the event and other detials. However, in general, you'll leverage the corresponding [Python Classes & Utility Methods](#ppython-classes--utility-methods) instead of handcrafting the JSON.

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

Other than `eventHeader`, all other entity-specific fields (e.g. `dataset`, `dashboard`, etc) are optional. In fact, it is quite common for each `MetadataChangeEvent` to contain metadata on a single type of entity.

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

You use `logicalId` to specify which dataset the metadata aspects are associated to, and the aspects in this case can be a combination of `properties`, `schema`, `statistics` etc. 


## Entity ID

There are two types of ID used 


## Python Classes & Utility Methods

