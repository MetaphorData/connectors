
            WITH c AS (
                SELECT
                    catalog_name,
                    catalog_owner,
                    comment
                FROM system.information_schema.catalogs
                WHERE catalog_name <> 'system'
            ),

            t AS (
                SELECT
                    catalog_name,
                    struct(tag_name, tag_value) as tag
                FROM system.information_schema.catalog_tags
                WHERE catalog_name <> 'system'
            )

            SELECT
                c.catalog_name AS catalog_name,
                first(c.catalog_owner) AS catalog_owner,
                first(c.comment) AS comment,
                collect_list(t.tag) AS tags
            FROM c
            LEFT JOIN t
            ON c.catalog_name = t.catalog_name
            GROUP BY c.catalog_name
            ORDER by c.catalog_name
        