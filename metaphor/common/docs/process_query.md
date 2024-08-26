# Process Query Config

Controls whether the crawler should process the SQL queries before storing the MCEs. Default is to not process the queries.

```yaml
process_query:
  redact_literals:
    enabled: <true | false> # Whether to redact the literal values. Default is `false`.

    placeholder_literal: <placeholder literal>  # The redacted values will be replaced by this placeholder string. Default is '<REDACTED>'.

  ignore_insert_values_into: <true | false>  #  Ignore `INSERT INTO ... VALUES` expressions. These expressions don't have any lineage information, and are often very large in size. Default is `false`.
```

If any of the following boolean values is set to true, crawler will process the incoming SQL queries:

- `redact_literals.enabled`
- `ignore_insert_values_into`
