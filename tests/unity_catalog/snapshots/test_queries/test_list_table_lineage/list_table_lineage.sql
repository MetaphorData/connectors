
            SELECT
                source_table_full_name,
                target_table_full_name
            FROM system.access.table_lineage
            WHERE
                target_table_catalog = 'c' AND
                target_table_schema = 's' AND
                source_table_full_name IS NOT NULL AND
                event_time > date_sub(now(), 7)
            GROUP BY
                source_table_full_name,
                target_table_full_name
        