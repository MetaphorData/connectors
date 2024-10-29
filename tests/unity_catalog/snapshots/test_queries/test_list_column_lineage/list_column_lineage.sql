
            SELECT
                source_table_full_name,
                source_column_name,
                target_table_full_name,
                target_column_name
            FROM system.access.column_lineage
            WHERE
                target_table_catalog = 'catalog' AND
                target_table_schema = 'schema' AND
                source_table_full_name IS NOT NULL AND
                event_time > date_sub(now(), 7)
            GROUP BY
                source_table_full_name,
                source_column_name,
                target_table_full_name,
                target_column_name
        