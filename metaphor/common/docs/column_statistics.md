# Column Statistics

Profiling large tables can take a lot of time and resources. Fortunately, most data warehouses and databases pre-compute some column statistics (e.g. null count, min, and max) and can readily return them without resorting to a full table scan. These are enabled by default in the `column_statistics` config. To compute the other more expensive statistics, you'll need to manually enable them in the config.

```yaml
column_statistics:
    # Compute null and non-null counts (default True)
    null_count: true

    # Compute unique value counts (default False)
    unique_count: false

    # Compute min value (default True)
    min_value: true

    # Compute max value (default True)
    max_value: true

    # Compute average value (default False)
    avg_value: false

    # Compute standard deviation (default False)
    std_dev: false
```
