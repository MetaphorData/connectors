WITH q AS (
    SELECT
        *
    FROM
        (
            SELECT
                foo,
                bar
            FROM
                (
                    SELECT
                        *
                    FROM
                        db.sch.src
                    WHERE
                        col > '<REDACTED>'
                        AND col < '<REDACTED>'
                )
            WHERE
                col2 = '<REDACTED>'
        )
    WHERE
        foo = '<REDACTED>'
)
SELECT
    foo,
    bar
FROM
    q
WHERE
    bar <> '<REDACTED>'