# Migration Guide

## 0.13 to 0.14

Breaking changes: https://github.com/MetaphorData/connectors/issues/621

A deprecated connector has been dropped:
- [Drop redshift.lineage connector](https://github.com/MetaphorData/connectors/pull/854)

We have also introduced [breaking changes](https://github.com/MetaphorData/connectors/pull/857) to the configs for `unity_catalog` & `unity_catalog.profile` connectors. Please replace `host`, `warehouse_id`, and `cluster_path` with `hostname` & `http_path`. Use [this page](https://docs.databricks.com/en/integrations/compute-details.html) to find the values for the new configs.

## 0.12 to 0.13

Breaking changes: https://github.com/MetaphorData/connectors/issues/523

A few deprecated connectors & configs have been dropped:
- [Drop support for dbt 0.x](https://github.com/MetaphorData/connectors/pull/561)
- [Drop airflow plugin](https://github.com/MetaphorData/connectors/pull/562)
- [Drop support for API sink ](hhttps://github.com/MetaphorData/connectors/pull/565)
- [Drop job_id from dbt cloud config](https://github.com/MetaphorData/connectors/pull/619)
- [Drop unused data_platform field from MC's config](https://github.com/MetaphorData/connectors/pull/620)

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
