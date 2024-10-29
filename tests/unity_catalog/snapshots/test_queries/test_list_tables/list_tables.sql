
            WITH
            t AS (
                SELECT
                    table_catalog,
                    table_schema,
                    table_name,
                    table_type,
                    table_owner,
                    comment,
                    data_source_format,
                    storage_path,
                    created,
                    created_by,
                    last_altered,
                    last_altered_by
                FROM system.information_schema.tables
                WHERE
                    table_catalog = %(catalog)s AND
                    table_schema = %(schema)s
            ),

            tt AS (
                SELECT
                    catalog_name AS table_catalog,
                    schema_name AS table_schema,
                    table_name AS table_name,
                    collect_list(struct(tag_name, tag_value)) as tags
                FROM system.information_schema.table_tags
                WHERE
                    catalog_name = %(catalog)s AND
                    schema_name = %(schema)s
                GROUP BY catalog_name, schema_name, table_name
            ),

            v AS (
                SELECT
                    table_catalog,
                    table_schema,
                    table_name,
                    view_definition
                FROM system.information_schema.views
                WHERE
                    table_catalog = %(catalog)s AND
                    table_schema = %(schema)s
            ),

            tf AS (
                SELECT
                    t.table_catalog,
                    t.table_schema,
                    t.table_name,
                    t.table_type,
                    t.table_owner,
                    t.comment,
                    t.data_source_format,
                    t.storage_path,
                    t.created,
                    t.created_by,
                    t.last_altered,
                    t.last_altered_by,
                    v.view_definition,
                    tt.tags
                FROM t
                LEFT JOIN v
                ON
                    t.table_catalog = v.table_catalog AND
                    t.table_schema = v.table_schema AND
                    t.table_name = v.table_name
                LEFT JOIN tt
                ON
                    t.table_catalog = tt.table_catalog AND
                    t.table_schema = tt.table_schema AND
                    t.table_name = tt.table_name
            ),

            c AS (
                SELECT
                    table_catalog,
                    table_schema,
                    table_name,
                    column_name,
                    data_type,
                    CASE
                        WHEN numeric_precision IS NOT NULL THEN numeric_precision
                        WHEN datetime_precision IS NOT NULL THEN datetime_precision
                        ELSE NULL
                    END AS data_precision,
                    is_nullable,
                    comment
                FROM system.information_schema.columns
                WHERE
                    table_catalog = %(catalog)s AND
                    table_schema = %(schema)s
            ),

            ct AS (
                SELECT
                    catalog_name AS table_catalog,
                    schema_name AS table_schema,
                    table_name,
                    column_name,
                    collect_list(struct(tag_name, tag_value)) as tags
                FROM system.information_schema.column_tags
                WHERE
                    catalog_name = %(catalog)s AND
                    schema_name = %(schema)s
                GROUP BY catalog_name, schema_name, table_name, column_name
            ),

            cf AS (
                SELECT
                    c.table_catalog,
                    c.table_schema,
                    c.table_name,
                    collect_list(struct(
                        c.column_name,
                        c.data_type,
                        c.data_precision,
                        c.is_nullable,
                        c.comment,
                        ct.tags
                    )) as columns
                FROM c
                LEFT JOIN ct
                ON
                    c.table_catalog = ct.table_catalog AND
                    c.table_schema = ct.table_schema AND
                    c.table_name = ct.table_name AND
                    c.column_name = ct.column_name
                GROUP BY c.table_catalog, c.table_schema, c.table_name
            )

            SELECT
                tf.table_catalog AS catalog_name,
                tf.table_schema AS schema_name,
                tf.table_name AS table_name,
                tf.table_type AS table_type,
                tf.table_owner AS owner,
                tf.comment AS table_comment,
                tf.data_source_format AS data_source_format,
                tf.storage_path AS storage_path,
                tf.created AS created_at,
                tf.created_by AS created_by,
                tf.last_altered as updated_at,
                tf.last_altered_by AS updated_by,
                tf.view_definition AS view_definition,
                tf.tags AS tags,
                cf.columns AS columns
            FROM tf
            LEFT JOIN cf
            ON
                tf.table_catalog = cf.table_catalog AND
                tf.table_schema = cf.table_schema AND
                tf.table_name = cf.table_name
            ORDER by tf.table_catalog, tf.table_schema, tf.table_name
        