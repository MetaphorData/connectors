# Snowflake Connector

This connector extracts technical metadata from a Snowflake account using [Snowflake Connector](https://docs.snowflake.com/en/user-guide/python-connector.html).

## Setup

We recommend creating a dedicated Snowflake user with limited permissions for the connector to use by running the following statements using `ACCOUNTADMIN` role:

```sql
set warehouse = '<warehouse>';
set db = '<database>';

-- Create metaphor_role
create role metaphor_role comment = 'Limited access role for Metaphor connector';
grant usage on warehouse identifier($warehouse) to role metaphor_role;
grant usage on all schemas in database identifier($db) to role metaphor_role;
grant usage on future schemas in database identifier($db) to role metaphor_role;
grant references on all tables in database identifier($db) to role metaphor_role;
grant references on future tables in database identifier($db) to role metaphor_role;
grant references on all views in database identifier($db) to role metaphor_role;
grant references on future views in database identifier($db) to role metaphor_role;
grant references on all materialized views in database identifier($db) to role metaphor_role;
grant references on future materialized views in database identifier($db) to role metaphor_role;

-- Create metaphor_user
create user metaphor_user
    password = '<password>'
    default_warehouse = $warehouse
    default_role = metaphor_role
    comment = 'User for Metaphor connector';
grant role metaphor_role to user metaphor_user;
```

### Key Pair Authentication (Optional)

If you intend to use key pair authentication instead of password, follow the [Snowflake instruction](https://docs.snowflake.com/en/user-guide/key-pair-auth.html) to generate a key pair. After that, assign the public key to the user using the following command:

```sql
alter user metaphor_user set rsa_public_key='<public_key_content>';
```

## Config File

Create a YAML config file based on the following template.

### Required Configurations

If using user password authentication:

```yaml
account: <snowflake_account>
user: <snowflake_username>
password: <snowflake_password>
default_database: <default_database_for_connections>
output:
  file:
    path: <path_to_output_file>
```

If using key pair authentication:

```yaml
account: <snowflake_account>
user: <snowflake_username>
private_key:
  key_file: <private_key_file>
  passphrase: <private_key_encoding_passphrase>
default_database: <default_database_for_connections>
output:
  file:
    path: <path_to_output_file>
```

The `private_key.passphrase` is only needed if using encrypted version of the private key. Otherwise, it can be omitted from the config.

See [Common Configurations](../common/README.md) for more information on `output`.

### Optional Configurations

#### Filter

By default, the connector will extract metadata from all tables/views in all schemas and databases. You can optionally limit it by specifying the `filter` option. For example, the following config will only include tables/views from database `db1` and `db2`:

```yaml
filter:
  includes:
    db1:    
    db2:
```

You can also exclude only specific databases, schemas, or tables/views. For example, the following will include all tables/views except `db1.*`, `db2.schema1.*`, and `db3.schema1.table1`:

```yaml
filter:
  excludes:
    db1:  
    db2:
      schema1:
    db3:
      schema1:
        - table1
```

Note that when there's an overlap between `includes` and `excludes`, the latter will always take precedence. For instance, the following config will include all tables under `db1`, except `db1.schema1.table1` and `db1.schema1.table2`:

```
filter:
  includes:
    db1:
  excludes:
    db1:
      schema1:  
        - table1
        - table2

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
python -m metaphor.snowflake <config_file>
```

Manually verify the output after the run finishes.
