# Sampling

For profiling connector

For tables with large number of rows, it can take a long time to profile. To speed up the process, we can choose to do percentage-based random sampling on the data by setting `sampling.percentage` and `sampling.threshold`:

```yaml
sampling:
  percentage: <number between 0 and 100>
  threshold: <number of rows>
```

For example, `sampling.percentage = 1` means random sampling of 1% rows in the table. And, `sampling.threshold = 100000` this means sampling won't apply to the table less than 100K rows, in that case, we do complete table profiling.

## Supported Connectors

- [metaphor.bigquery.profile](metaphor/bigquery/profile/README.md)
