# Migration Guide

## 0.11 to 0.12

Breaking changes: https://github.com/MetaphorData/connectors/issues/310

Python 3.7 is no longer supported starting 0.12.

A few deprecated connectors & configs have been dropped:
- [Drop unused query & usage connectors](https://github.com/MetaphorData/connectors/pull/521)
- [Drop batch_size from FileSinkConfig](https://github.com/MetaphorData/connectors/pull/522)
- [Drop project_id from BigQueryRunConfig](https://github.com/MetaphorData/connectors/pull/525)

We also renamed the `metaphor.manual` namespace to `metaphor.custom`. This means you'll need to change the name of the connectors when running, e.g.

```sh
metaphor custom.governance <config_file>
metaphor custom.lineage <config_file>
```

## 0.10 to 0.11

Breaking changes: https://github.com/MetaphorData/connectors/issues/142

The most notiable change made in 0.11 is the way connectors are invoked. Instead of running individual module using Python, e.g.

```sh
python -m metaphor.bigquery <config_file>
```

a unified CLI is used to run any connector, e.g.

```sh
metaphor bigquery <config_file>
```

## 0.9 to 0.10

Breaking changes: https://github.com/MetaphorData/connectors/issues/95

All the breaking changes in 0.10 is related to config options, specifically
- [Dropping the support of JSON-based config file](https://github.com/MetaphorData/connectors/pull/139)
- [Output is specified a directory, instead of a file](https://github.com/MetaphorData/connectors/pull/138)
- [A unified dataset filter config](https://github.com/MetaphorData/connectors/pull/128)
