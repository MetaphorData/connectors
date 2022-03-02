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
grant usage on database identifier($db) to role metaphor_role;
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
    directory: <output_directory>
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
    directory: <output_directory>
```

The `private_key.passphrase` is only needed if using encrypted version of the private key. Otherwise, it can be omitted from the config.

See [Common Configurations](../common/README.md) for more information on `output`.

### Optional Configurations

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

See [Filter Configurations](../common/docs/filter.md) for more information on the optional `filter` config.

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv). Make sure to include either `all` or `snowflake` extra.

To test the connector locally, change the config file to output to a local path and run the following command

```shell
python -m metaphor.snowflake <config_file>
```

Manually verify the output after the run finishes.
