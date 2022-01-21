# Sampling Config

For tables with a large number of rows, it can take a long time to profile. To speed up the process, we can choose to do a percentage-based random sampling on the data by setting `sampling.percentage` and `sampling.threshold`:

```yaml
sampling:
  percentage: <number between 1 and 100>
  threshold: <number of rows>
```

For example, `sampling.percentage = 1` means random sampling of 1% of rows in the table, and `sampling.threshold = 100000` means sampling won't apply to tables less than 100K rows, i.e., profiling the whole table.
