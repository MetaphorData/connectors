# MongoDB Connector

This connector extracts technical metadata from a MongoDB database.

## Setup

Your user should be granted with enough permission to be able to perform `listDatabases` and do `collstats` on all non-system collections.

To grant permission to your user, run the following command in `mongosh`:

```js
db.getSiblingDb("admin").grantRoleToUser("<USER_NAME>", ["clusterManager"]);
```

For the complete list of available roles, visit [the official MongoDB manual](https://www.mongodb.com/docs/manual/reference/built-in-roles/).

### Required Configurations

```yaml
uri: <uri> # The connection URI.
auth_mechanism: <auth_mechanism> # The authentication mechanism. Allowed values are "GSSAPI", "MONGODB-CR", "MONGODB-OIDC", "MONGODB-X509", "MONGODB-AWS", "PLAIN", "SCRAM-SHA-1", "SCRAM-SHA-256", "DEFAULT". Default is "DEFAULT".
tls: <boolean> # Whether to set TLS when connecting to MongoDB. Default is True.

infer_schema_sample_size: <int> # Number of documents to sample in a collection in order to infer the schema. Set this to `null` to disable sampling and use all documents in the collections. To disable schema inference altogether, set this to 0. Default is 1000.
excluded_databases: # Extra databases to ignore. The system databases "admin", "config", "local", "system" are always excluded.
  - db1
  - db2
excluded_collections: # Extra collections to ignore. Note that the system specific collections (`system.views`, `system.profile`, etc.) are always ignored, see https://www.mongodb.com/docs/manual/reference/system-collections/#database-specific-collections for more details.
  - coll1
  - coll2
```

### Optional Configurations

#### Output Destination

See [Output Config](../common/docs/output.md) for more information.

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv). Make sure to include either `all` or `mongodb` extra.

Run the following command to test the connector locally:

```shell
metaphor mongodb <config_file>
```

Manually verify the output after the command finishes.
