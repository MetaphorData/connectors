COPY INTO tgt
(
    foo,
    bar
)
FROM
    (
        SELECT
            *
        FROM
            src
    )
FILES = ('1') REGION = 'us-east-1' CREDENTIALS = (AWS_KEY_ID = 'id' AWS_SECRET_KEY = 'key' AWS_TOKEN='tok') ENCRYPTION = (MASTER_KEY = '') FILE_FORMAT = ( BINARY_FORMAT = BASE64 TYPE = 'csv' ESCAPE = NONE ESCAPE_UNENCLOSED_FIELD = NONE FIELD_OPTIONALLY_ENCLOSED_BY = '\"' COMPRESSION = 'zstd' NULL_IF = 'null-vba3aoqjpgeovgjvmzn5cklcstanclr' SKIP_HEADER = 1 ) PURGE = TRUE ON_ERROR = abort_statement