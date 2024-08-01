# Oracle Connector

This connector extracts technical metadata from an Oracle database using [oracledb](https://oracle.github.io/python-oracledb/) library.

## Setup

We recommend you to create a role to manage privileges for Metaphor Oracle connector.

```sql
-- Step 1: Create the role
CREATE ROLE metaphor;

-- Grant necessary privileges to the role
GRANT CREATE SESSION TO metaphor; -- Allows login
GRANT SELECT_CATALOG_ROLE TO metaphor; -- Allows getting DDL

-- Step 2: Create the user
CREATE USER metaphor_user IDENTIFIED BY my_password;

-- Step 3: Assign the role to the user
GRANT metaphor TO metaphor_user;

```

We also need `SELECT` privileges on tables, materialized views and views you care about to list them and get the metadata of them.

```sql
BEGIN
    FOR t IN (SELECT table_name, owner
              FROM all_tables 
              WHERE owner = 'TARGET_SCHEMA') 
    LOOP
        EXECUTE IMMEDIATE 'GRANT SELECT ON ' || t.owner || '.' || t.table_name || ' TO metaphor';
    END LOOP;
END;
```

## Config File

Create a YAML config file based on the following template.

### Required Configurations

If using user password authentication:

```yaml
host: <hostname>  # the host the crawler will connect to
user: <username>
password: <password>
database: <default_database_for_connections>
```

### Optional Configurations

By default, the connector will connect using the default Oracle port 1521. You can change it using the following config:

```yaml
port: <port_number>
```

When connecting to the database indirectly, such as through a tunnel, it's important to configure the `alternative_host` parameter. This ensures that a stable hostname is used to construct unique identifiers for tables, regardless of the connection method.

For example, if you connect to the database using the IP and port host: `10.1.1.200`, you should also set the `alternative_host` parameter to a stable hostname like `dev.oracle.foo.com`. This hostname will be used for table identification and consistency.

```yaml
alternative_host: <hostname>
```

See [Filter Configurations](../common/docs/filter.md) for more information on the optional `filter` config.

#### Output Destination

See [Output Config](../common/docs/output.md) for more information on the optional `output` config.

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv). Make sure to include either `all` or `oracle` extra.

Run the following command to test the connector locally:

```shell
metaphor oracle <config_file>
```

Manually verify the output after the run finishes.
