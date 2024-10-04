# Snowflake Connector

This connector extracts technical metadata from a Snowflake account using [Snowflake Connector](https://docs.snowflake.com/en/user-guide/python-connector.html).

## Setup

We recommend creating a dedicated Snowflake user with limited permissions for the connector to use by running the following statements using `ACCOUNTADMIN` role:

```sql
use role ACCOUNTADMIN;

set warehouse = '<warehouse>';
set role = 'metaphor_role';
set user = 'metaphor_user';
set password = '<password>';

-- Create metaphor_user
create user identifier($user)  
    password = $password  
    default_warehouse = $warehouse  
    default_role = $role  
    comment ='User for Metaphor crawler';  

-- Create metaphor_role  
create role identifier($role) comment ='Limited access role for Metaphor connector';  
grant role identifier($role) to user identifier($user);
grant usage on warehouse identifier($warehouse) to role identifier($role);  

-- Grant privilege to access Snowflake Account Usage views:
grant imported privileges on database snowflake to role identifier($role);
```

For each database, including the inbound shared databases, run the following statements to grant the required privileges:

```sql
set db = '<database>';

-- Grant usage & references privileges to query information_schema
grant usage on warehouse identifier($warehouse) to role identifier($role);
grant usage on database identifier($db) to role identifier($role);
grant usage on all schemas in database identifier($db) to role identifier($role);
grant usage on future schemas in database identifier($db) to role identifier($role);
grant references on all tables in database identifier($db) to role identifier($role);
grant references on future tables in database identifier($db) to role identifier($role);
grant references on all views in database identifier($db) to role identifier($role);
grant references on future views in database identifier($db) to role identifier($role);
grant references on all materialized views in database identifier($db) to role identifier($role);
grant references on future materialized views in database identifier($db) to role identifier($role);

-- (Optional) Grant privilege to "show streams"
grant select on all streams in database identifier($db) to role identifier($role);
grant select on future streams in database identifier($db) to role identifier($role);

-- (Optional) Grant privilege to "show iceberg tables"
grant select on all iceberg tables in database identifier($db) to role identifier($role);
grant select on future iceberg tables in database identifier($db) to role identifier($role);
```

### Key Pair Authentication (Optional)

If you intend to use key pair authentication instead of password, follow the [Snowflake instruction](https://docs.snowflake.com/en/user-guide/key-pair-auth.html) to generate a key pair. After that, assign the public key to the user using the following command:

```sql
alter user identifier($user) set rsa_public_key='<public_key_content>';
```

## Config File

Create a YAML config file based on the following template.

### Required Configurations

If using user password authentication:

```yaml
account: <snowflake_account>
user: <snowflake_username>
password: <snowflake_password>
role: <snowflake_role> # Optional. Will use default role if not specified.
default_database: <default_database_for_connections>
```

If using key pair authentication:

```yaml
account: <snowflake_account>
user: <snowflake_username>
private_key:
  key_file: <private_key_file>
  passphrase: <private_key_encoding_passphrase>
role: <snowflake_role> # Optional. Will use default role if not specified.
default_database: <default_database_for_connections>
```

The `private_key.passphrase` is only needed if using encrypted version of the private key. Otherwise, it can be omitted from the config.

You can also specify the content of the private key file directly in the config like this:

```yaml
private_key:
  key_data: |
    -----BEGIN ENCRYPTED PRIVATE KEY-----
    ...
    -----END ENCRYPTED PRIVATE KEY-----
```

### Optional Configurations

#### Output Destination

See [Output Config](../common/docs/output.md) for more information.

#### Filtering

See [Filter Config](../common/docs/filter.md) for more information on the optional `filter` config.

#### Alternative ACCOUNT_USAGE Schema

By default, the connector will read the [QUERY_HISTORY](https://docs.snowflake.com/en/sql-reference/account-usage/query_history), [ACCESS_HISTORY](https://docs.snowflake.com/en/sql-reference/account-usage/access_history), and [TAG_REFERENCES](https://docs.snowflake.com/en/sql-reference/account-usage/tag_references) views from [SNOWFLAKE.ACCOUNT_USAGE](https://docs.snowflake.com/en/sql-reference/account-usage) schema. If you do not wish to grant read access to the entire SNOWFLAKE database, you can mirror these views to a different schema and ask the connector to read from it instead:

```yaml
account_usage_schema: <db_name>.<schema_name>
```

#### Tag Assignment

See [Tag Matcher Config](../common/docs/tag_matcher.md) for more information on the optional `tag_matcher` config.

#### Disable Platform Tags Collection

To stop the crawler from collecting platform tags from Snowflake, set `collect_tags` to `False`:

```yaml
collect_tags: false # Default is true.
```

#### Query Logs

By default, the snowflake connector will fetch a full day's query logs from yesterday, to be analyzed for additional metadata, such as dataset usage and lineage information. To backfill log data, one can set `lookback_days` to the desired value. To turn off query log fetching, set `lookback_days` to 0.  

```yaml

query_log:
  # (Optional) Number of days of query logs to fetch. Default to 1. If 0, the no query logs will be fetched.
  lookback_days: <days>
    
  # (Optional) A list of users whose queries will be excluded from the log fetching.
  excluded_usernames:
    - <user_name1>
    - <user_name2>
  
  # (Optional) The number of query logs to fetch from Snowflake in one batch. Default to 100000.
  fetch_size: <number_of_logs>
```

##### Process Query Config

See [Process Query](../common/docs/process_query.md) for more information on the optional `process_query_config` config.

#### Concurrency

The max number of concurrent queries to the snowflake database can be configured as follows,

```yaml
max_concurrency: <max_number_of_queries> # Default to 10
```

#### Query Tag

Each query issued by snowflake connectors can be tagged with a query tag. It can be configured as follows,

```yaml
query_tag: <query_tag> # Default to 'MetaphorData'
```

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv). Make sure to include either `all` or `snowflake` extra.

Run the following command to test the connector locally:

```shell
metaphor snowflake <config_file>
```

Manually verify the output after the run finishes.
