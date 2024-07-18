# Process Query Config

Controls whether the crawler should process the SQL queries before storing the MCEs. Default is to not process the queries.

```yaml
process_query:
  redact_literal_values_in_where_clauses: <true | false>  # Whether to redact all literal values in WHERE clauses. Default is `false`.

  redacted_literal_placeholder: <redacted literal placeholder>  # The redacted values will be replaced by this placeholder string. Default is '<REDACTED>'.

  ignore_insert_values_into: <ignore_insert_values_into>  #  Ignore `INSERT INTO ... VALUES` expressions. These expressions don't have any lineage information, and are often very large in size. Default is `false`.
```

If any of the following boolean values is set to true, crawler will process the incoming SQL queries:

- `redact_literal_values_in_where_clauses`
- `ignore_insert_values_into`
