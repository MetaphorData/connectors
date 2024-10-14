
            WITH v AS (
                SELECT
                    volume_catalog,
                    volume_schema,
                    volume_name,
                    volume_type,
                    volume_owner,
                    comment,
                    created,
                    created_by,
                    last_altered,
                    last_altered_by,
                    storage_location
                FROM system.information_schema.volumes
                WHERE volume_catalog = %(catalog)s AND volume_schema = %(schema)s
            ),

            t AS (
                SELECT
                    catalog_name,
                    schema_name,
                    volume_name,
                    struct(tag_name, tag_value) as tag
                FROM system.information_schema.volume_tags
                WHERE catalog_name = %(catalog)s AND schema_name = %(schema)s
            )

            SELECT
                first(v.volume_catalog) AS volume_catalog,
                first(v.volume_schema) AS volume_schema,
                v.volume_name AS volume_name,
                first(v.volume_type) AS volume_type,
                first(v.volume_owner) AS volume_owner,
                first(v.comment) AS comment,
                first(v.created) AS created,
                first(v.created_by) AS created_by,
                first(v.last_altered) AS last_altered,
                first(v.last_altered_by) AS last_altered_by,
                first(v.storage_location) AS storage_location,
                collect_list(t.tag) AS tags
            FROM v
            LEFT JOIN t
            ON
                v.volume_catalog = t.catalog_name AND
                v.volume_schema = t.schema_name AND
                v.volume_name = t.volume_name
            GROUP BY v.volume_name
            ORDER BY v.volume_name
        