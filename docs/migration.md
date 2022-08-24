# Migration Guide

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
