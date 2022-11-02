# Snowflake Connector

This connector extracts technical metadata from a Snowflake account using [Snowflake Connector](https://docs.snowflake.com/en/user-guide/python-connector.html).

## Setup

We recommend creating a dedicated Snowflake user with limited permissions for the connector to use by running the following statements using `ACCOUNTADMIN` role:

```sql
use role ACCOUNTADMIN;

set warehouse = '<warehouse>';
set db = '<database>';
set role = 'metaphor_role';
set user = 'metaphor_user';
set password = '<password>';

-- Create metaphor_role
create role identifier($role) comment = 'Limited access role for Metaphor connector';
grant usage on warehouse identifier($warehouse) to role identifier($role);
grant usage on database identifier($db) to role identifier($role);
-- Grant permissions to access the snowflake "Account Usage" views:
grant imported privileges on database snowflake to role identifier($role);
-- If there are inbound shared databases, grant permissions to "show shares"
grant import share on account to identifier($role);

-- Create metaphor_user
create user identifier($user) 
    password = $password
    default_warehouse = $warehouse
    default_role = $role
    comment = 'User for Metaphor connector';
grant role identifier($role) to user identifier($user);
```

### Key Pair Authentication (Optional)

If you intend to use key pair authentication instead of password, follow the [Snowflake instruction](https://docs.snowflake.com/en/user-guide/key-pair-auth.html) to generate a key pair. After that, assign the public key to the user using the following command:

```sql
alter user identifier($user) set rsa_public_key='<public_key_content>';
```

### Fetching extra info (Optional)

If you intend to fetch extra table information such as LAST_CHANGE_COMMIT_TIME or DDL (enabled by default), following additional grant needs to be given to the newly created role.

```sql
grant usage on all schemas in database identifier($db) to role identifier($role);
grant usage on future schemas in database identifier($db) to role identifier($role);
grant references on all tables in database identifier($db) to role identifier($role);
grant references on future tables in database identifier($db) to role identifier($role);
grant references on all views in database identifier($db) to role identifier($role);
grant references on future views in database identifier($db) to role identifier($role);
grant references on all materialized views in database identifier($db) to role identifier($role);
grant references on future materialized views in database identifier($db) to role identifier($role);
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
output:
  file:
    directory: <output_directory>
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
output:
  file:
    directory: <output_directory>
```

The `private_key.passphrase` is only needed if using encrypted version of the private key. Otherwise, it can be omitted from the config.

See [Output Config](../common/docs/output.md) for more information on `output`.

### Optional Configurations

See [Filter Configurations](../common/docs/filter.md) for more information on the optional `filter` config.

#### Fetch Extra Table Information

By default, the crawler fetches extra table information such as LAST_CHANGE_COMMIT_TIME or DDL. This requires extra grant to all the tables. To disable this, add the following:

```yaml
fetch_extra_table_info: false
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

#### Concurrency

The max number of concurrent queries to the snowflake database can be configured as follows,

```yaml
max_concurrency: <max_number_of_queries> # Default to 10
```

#### Query Tag

Each query issued by snowflake connectors can be tagged with a query tag. It can be configured as follows,

```yaml
query_tag: <query_taqg> # Default to 'MetaphorData'
```

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv). Make sure to include either `all` or `snowflake` extra.

To test the connector locally, change the config file to output to a local path and run the following command

```shell
metaphor snowflake <config_file>
```

Manually verify the output after the run finishes.
