# Kafka Connector

This connector extracts technical metadata from Kafka using [Confluent's Python Client](https://github.com/confluentinc/confluent-kafka-python).

## Setup

To run a Kafka cluster locally, follow the instructions below:

1. Start a Kafka cluster (broker + schema registry + REST proxy) locally via docker-compose:
```shell
$ docker-compose --file metaphor/kafka/docker-compose.yml up -d
```
  - Broker is on port 9092.
  - Schema registry is on port 8081.
  - REST proxy is on port 8082.
2. Find the cluster ID:  
```shell
$ curl -X GET --silent http://localhost:8082/v3/clusters/ | jq '.data[].cluster_id'
```
3. Register a new topic via the REST proxy:
```shell
curl -X POST -H "Content-Type: application/json" http://localhost:8082/v3/clusters/<YOUR CLUSTER ID>/topics -d '{"topic_name": "<YOUR TOPIC NAME>"}'| jq .
```
4. Register a schema to the registry:
```shell
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" --data '{"schema": <SCHEMA AS STRING>}' http://localhost:8081/subjects/<YOUR TOPIC NAME>-<key|value>/version
```
  - It is possible to have schema with name different to the topic. See `Topic <-> Schema Subject Mapping` section below for more info.

## Config File

Create a YAML config file based on the following template.

### Required Configurations

You must specify at least one bootstrap server, i.e. a pair of host and port pointing to the Kafka broker instance. You must also specify a URL for the schema registry.

```yaml
bootstrap_servers:
  - host: <host>
    port: <port>
schema_registry_url: <schema_registry_url> # Schema Registry URL. Schema registry client supports URL with basic HTTP authentication values, i.e. `http://username:password@host:port`.
output:
  file:
    directory: <output_directory>
```

See [Output Config](../common/docs/output.md) for more information on `output`.

### Optional Configurations

#### Kafka Admin Client

##### Common SASL Authentication Configurations

Some most commonly used SASL authentication configurations have their own section:

```yaml
sasl_config:
  username: <username> # SASL username for use with the `PLAIN` and `SASL-SCRAM-..` mechanisms.
  password: <password> # SASL password for use with the `PLAIN` and `SASL-SCRAM-..` mechanisms.
  mechanism: <mechanism> # SASL mechanism to use for authentication. Supported: `GSSAPI`, `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`, `OAUTHBEARER`. Default: `GSSAPI`.
```

##### Other Configurations

For other configurable values, please use `extra_admin_client_config` field:

```yaml
extra_admin_client_config:
  sasl.kerberos.service.name: "kafka"
  sasl.kerberos.principal: "kafkaclient"
  ...
```

Visit [https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md) for full list of available Kafka client configurations.

#### Filtering

You can filter the topics you want to include in the ingested result:

```yaml
filter:
  includes: <set of patterns to include>
  excludes: <set of patterns to exclude>
```

By default the following topics are excluded:

- `_schema`
- `__consumer_offsets`

#### Topic <-> Schema Subject Mapping

Kafka messages are sent as key / value pairs, and both can have their schemas defined in the schema registry. There are three strategies to map topic to schema subjects:

##### Strategies

###### Topic Name Strategy (Default)

For a topic `foo`, the subjects for the schemas for the messages sent through this topic would be `foo-key` (the key schema subject) and `foo-value` (the value schema subject).

###### Record Name Strategy

It is possible for a topic to have more than one schema. In that case this strartegy can be useful. To enable this as default, add the following in the configuration file:

```yaml
default_subject_name_strategy: RECORD_NAME_STRATEGY
topic_naming_strategies:
  foo:
    records:
      - bar
      - baz
```

This means topic `foo` can transmit the following schemas:

- `bar-key`
- `bar-value`
- `baz-key`
- `baz-value`

###### Topic Record Name Strategy

This strategy is best demonstrated through an example:

```yaml
default_subject_name_strategy: TOPIC_RECORD_NAME_STRATEGY
topic_naming_strategies:
  foo:
    records:
      - bar
      - baz
  quax:
    records: [] # If list of record names is empty, we take all subjects that starts with "<topic>-" and ends with "-<key|value>" as topic subjects.
```

- For topic `foo`, the subjects it transmits are
  - `foo-bar-key`
  - `foo-bar-value`
  - `foo-baz-key`
  - `foo-baz-value`
- For topic `quax`, all subject that starts with `quax-` and ends with either `-key` or `-value` is considered a subject on topic `quax`.

##### Overriding Subject Name Strategy for Specific Topics

It is possible to override subject name strategy for specific topics:

```yaml
default_subject_name_strategy: RECORD_NAME_STRATEGY
topic_naming_strategies:
  foo:
    records:
      - bar
      - baz
  quax:
    override_subject_name_strategy: TOPIC_NAME_STRATEGY
```

- The following subjects are transmitted through topic `foo`:
  - `bar-key`
  - `bar-value`
  - `baz-key`
  - `baz-value`
- For topic `quax`, since it uses `TOPIC_NAME_STRATEGY`, connector will look for the following 2 subjects:
  - `quax-key`
  - `quax-value`

## Testing

Follow the [Installation](../../README.md) instructions to install `metaphor-connectors` in your environment (or virtualenv). Make sure to include either `all` or `kafka` extra.

To test the connector locally, change the config file to output to a local path and run the following command

```shell
metaphor kafka <config_file>
```

Manually verify the output after the run finishes.