# MongoDB Connector

This connector extracts technical metadata from a MongoDB database.

## Setup

### Required Configurations

```yaml
uri: <uri> # The connection URI.
auth_mechanism: <auth_mechanism> # The authentication mechanism. Allowed values are "GSSAPI", "MONGODB-CR", "MONGODB-OIDC", "MONGODB-X509", "MONGODB-AWS", "PLAIN", "SCRAM-SHA-1", "SCRAM-SHA-256", "DEFAULT". Default is "DEFAULT".
tls: <boolean> # Whether to set TLS when connecting to MongoDB. Default is False.

infer_schema_sample_size: <int> # Number of documents to sample in a collection in order to infer the schema. Set this to `null` to disable sampling and use all documents in the collections. To disable schema inference altogether, set this to 0. Default is 1000.
excluded_databases: # Databases to ignore. By default the databases "admin", "config", "local", "system" are excluded.
  - db1
  - db2
excluded_collections: # Collections to ignore.
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
