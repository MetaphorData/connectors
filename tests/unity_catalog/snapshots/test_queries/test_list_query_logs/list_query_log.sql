
            SELECT
                statement_id as query_id,
                executed_by as email,
                start_time,
                int(total_task_duration_ms/1000) as duration,
                read_rows as rows_read,
                produced_rows as rows_written,
                read_bytes as bytes_read,
                written_bytes as bytes_written,
                statement_type as query_type,
                statement_text as query_text
            FROM system.query.history
            WHERE
                Q.executed_by IN ('user1','user2') AND
                execution_status = 'FINISHED' AND
                start_time >= ? AND
                start_time < ?
        