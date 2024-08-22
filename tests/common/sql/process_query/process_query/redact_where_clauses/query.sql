INSERT INTO target_table (first_name, last_name, email, status)
SELECT
    first_name,
    last_name,
    email,
    CASE
        WHEN age < 18 THEN 'Minor'
        WHEN age >= 18 AND age < 65 THEN 'Adult'
        ELSE 'Senior'
    END as status
FROM source_table
WHERE email IS NOT NULL;