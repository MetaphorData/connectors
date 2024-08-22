INSERT INTO
    target_table (first_name, last_name, email, status)
SELECT
    first_name,
    last_name,
    email,
    CASE
        WHEN age < '<REDACTED>' THEN '<REDACTED>'
        WHEN age >= '<REDACTED>'
        AND age < '<REDACTED>' THEN '<REDACTED>'
        ELSE '<REDACTED>'
    END AS status
FROM
    source_table
WHERE
    NOT email IS NULL