# Filter Config

By default, the connector will extract metadata from all tables/views in all schemas and databases. You can optionally limit it by specifying the `filter` option. For example, the following config will only include tables/views from database `db1` and `db2`:

```yaml
filter:
  includes:
    db1:
    db2:
```

You can also exclude specific databases, schemas, or tables/views. For example, the following will include all tables/views except `db1.*`, `db2.schema1.*`, and `db3.schema1.table1`:

```yaml
filter:
  excludes:
    db1:
    db2:
      schema1:
    db3:
      schema1:
        - table1
```

Note that when there's an overlap between `includes` and `excludes`, the latter will always take precedence. For instance, the following config will include all tables under `db1`, except `db1.schema1.table1` and `db1.schema1.table2`:

```yaml
filter:
  includes:
    db1:
  excludes:
    db1:
      schema1:  
        - table1
        - table2
```

## Wildcards

Use wildcard characters (`*` and `?`) to match specific patterns. For example, the following config will exclude all schemas with a prefix `staging_` and a suffix `_temp`:

```yaml
filter:
  excludes:
    '*':
      'staging_*':
      '*_temp':
```

> Note: You must quote strings that start with wildcards or they'll be treated as [YAML aliases](https://www.educative.io/blog/advanced-yaml-syntax-cheatsheet#anchors).
