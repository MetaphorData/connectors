# Tag Matcher Config

To automatically assign governed tags to specific assets, you can specify a list of "Tag Matchers". For example, the following config will assign the tags `pii` and `core_tables` to all the tables under the database `db1`:

```yaml
tag_matchers:
  - pattern: 'db1.*'
    tags:
      - pii
      - core_tables
```

You can also use a more specific pattern, to mach tables under a particular schema  (e.g. `db1.schema1.*`) or simply use `*` to match all assets.

> Note: You must quote strings that start with wildcards or they'll be treated as [YAML aliases](https://www.educative.io/blog/advanced-yaml-syntax-cheatsheet#anchors).
