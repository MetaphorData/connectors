
            WITH s AS (
                SELECT
                    catalog_name,
                    schema_name,
                    schema_owner,
                    comment
                FROM system.information_schema.schemata
                WHERE catalog_name = %(catalog)s AND schema_name <> 'information_schema'
            ),

            t AS (
                SELECT
                    catalog_name,
                    schema_name,
                    struct(tag_name, tag_value) as tag
                FROM system.information_schema.schema_tags
                WHERE catalog_name = %(catalog)s AND schema_name <> 'information_schema'
            )

            SELECT
                first(s.catalog_name) AS catalog_name,
                s.schema_name AS schema_name,
                first(s.schema_owner) AS schema_owner,
                first(s.comment) AS comment,
                collect_list(t.tag) AS tags
            FROM s
            LEFT JOIN t
            ON s.catalog_name = t.catalog_name AND s.schema_name = t.schema_name
            GROUP BY s.schema_name
            ORDER by s.schema_name
        