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
                        col > 1234
                        AND col < 9999
                )
            WHERE
                col2 == 'some value'
        )
    WHERE
        foo == 'not really foo'
)
SELECT
    foo,
    bar
FROM
    q
WHERE
    bar != 'this cannot be bar'