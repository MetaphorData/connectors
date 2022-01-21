# Filter

By default, the connector will extract metadata from all tables/views in all schemas and databases. You can optionally limit it by specifying the `filter` option. For example, the following config will only include tables/views from database `db1` and `db2`:

```yaml
filter:
  includes:
    db1:    
    db2:
```

You can also exclude only specific databases, schemas, or tables/views. For example, the following will include all tables/views except `db1.*`, `db2.schema1.*`, and `db3.schema1.table1`:

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

## Supported Connectors

- [metaphor.postgresql](metaphor/postgresql/README.md)
- [metaphor.redshift](metaphor/redshift/README.md)
- [metaphor.snowflake](metaphor/snowflake/README.md)
- [metaphor.snowflake.profile](metaphor/snowflake/profile/README.md)
- [metaphor.snowflake.usage](metaphor/snowflake/usage/README.md)
